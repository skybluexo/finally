# Market Data Backend — Implementation Design

Complete design reference for the FinAlly market data subsystem. All code here
reflects the actual implementation in `backend/app/market/`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model)
4. [Price Cache — `cache.py`](#4-price-cache)
5. [Abstract Interface — `interface.py`](#5-abstract-interface)
6. [Seed Prices & Ticker Parameters — `seed_prices.py`](#6-seed-prices--ticker-parameters)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client)
9. [Factory — `factory.py`](#9-factory)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint)
11. [FastAPI Lifecycle Integration](#11-fastapi-lifecycle-integration)
12. [Watchlist Coordination](#12-watchlist-coordination)
13. [Public Package API — `__init__.py`](#13-public-package-api)
14. [Testing Strategy](#14-testing-strategy)
15. [Error Handling & Edge Cases](#15-error-handling--edge-cases)
16. [Configuration Summary](#16-configuration-summary)

---

## 1. Architecture Overview

The market data system follows a **strategy pattern** with a shared cache as the
single point of truth. Two data sources (simulator and Massive API) implement the
same abstract interface. All downstream code reads from the cache — it never
calls the data source directly.

```
┌─────────────────────────────────────────────────────────────────┐
│  MarketDataSource (ABC)                                         │
│  ├── SimulatorDataSource  (GBM simulator — default)             │
│  └── MassiveDataSource    (Polygon.io REST poller — optional)   │
│               │                                                 │
│               │  writes every 500ms                             │
│               ▼                                                 │
│         PriceCache  (thread-safe in-memory dict)                │
│               │                                                 │
│       ┌───────┼────────────────┐                                │
│       ▼       ▼                ▼                                │
│   SSE stream  Portfolio     Trade execution                     │
│   endpoint    valuation     (price lookup)                      │
└─────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

| Decision | Rationale |
|---|---|
| Strategy pattern | Both sources share one ABC; downstream code is source-agnostic |
| PriceCache as mediator | Producers and consumers are fully decoupled |
| Asyncio background tasks | Non-blocking; both sources run as `asyncio.Task` |
| `asyncio.to_thread` for Massive | The `massive` REST client is synchronous; runs in thread pool to avoid blocking the event loop |
| Version counter on cache | SSE uses `cache.version` to detect changes without comparing price dicts |
| Factory pattern | Source selection at startup based on `MASSIVE_API_KEY` env var |

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py          # Re-exports public API
      models.py            # PriceUpdate — immutable frozen dataclass
      cache.py             # PriceCache — thread-safe in-memory store
      interface.py         # MarketDataSource — abstract base class
      seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
      simulator.py         # GBMSimulator + SimulatorDataSource
      massive_client.py    # MassiveDataSource
      factory.py           # create_market_data_source() — env-driven factory
      stream.py            # create_stream_router() — FastAPI SSE endpoint
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
  market_data_demo.py      # Rich terminal demo (standalone script)
```

---

## 3. Data Model

**File:** `backend/app/market/models.py`

`PriceUpdate` is the only data structure that leaves the market data layer.
Everything downstream — SSE events, portfolio valuation, trade execution —
works with `PriceUpdate` objects.

```python
# backend/app/market/models.py

from __future__ import annotations
import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

**Why `frozen=True, slots=True`:**
- `frozen=True` — prevents accidental mutation; price snapshots are immutable facts
- `slots=True` — faster attribute access, lower memory per instance; important since thousands of these are created per minute
- `change`, `change_percent`, `direction` are computed properties rather than stored fields; they're always consistent with `price` and `previous_price`

**SSE payload shape** (what the frontend receives, one dict per ticker):
```json
{
  "ticker": "AAPL",
  "price": 190.50,
  "previous_price": 190.10,
  "timestamp": 1736000000.123,
  "change": 0.4,
  "change_percent": 0.2104,
  "direction": "up"
}
```

---

## 4. Price Cache

**File:** `backend/app/market/cache.py`

The `PriceCache` is the central bus between data producers (simulator or Massive
poller) and consumers (SSE endpoint, portfolio valuation, trade execution). It is
thread-safe: the Massive client calls `asyncio.to_thread`, which means writes can
come from a worker thread while reads happen on the event loop thread.

```python
# backend/app/market/cache.py

from __future__ import annotations
import time
from threading import Lock
from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Usage patterns:**

```python
cache = PriceCache()

# Producer writes
cache.update("AAPL", 190.50)

# Consumer reads
update = cache.get("AAPL")         # PriceUpdate | None
price  = cache.get_price("AAPL")   # float | None (convenience)
all_prices = cache.get_all()       # dict[str, PriceUpdate] — snapshot copy

# SSE change detection
if cache.version != last_version:
    last_version = cache.version
    # ... send update

# Watchlist removal
cache.remove("GOOGL")
```

**Thread safety:** All mutating methods and `get()` / `get_all()` acquire the
`Lock`. The `version` property reads a bare `int`, which is atomic on CPython
(GIL). This is fine for the current use case; if the project moves to no-GIL
Python (3.13+), `version` should also be read under the lock.

---

## 5. Abstract Interface

**File:** `backend/app/market/interface.py`

All downstream code that needs to add/remove tickers works through this interface,
never through the concrete classes directly.

```python
# backend/app/market/interface.py

from __future__ import annotations
from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Both implementations push price updates into a shared PriceCache on their
    own schedule. Downstream code never calls the data source for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

**Note:** `start()` and `stop()` are async; `get_tickers()` is synchronous (reads
in-memory state with no I/O). `add_ticker()` and `remove_ticker()` are async to
allow implementations to perform I/O on dynamic watchlist changes (e.g., a future
WebSocket-based source might need to send a subscription message).

---

## 6. Seed Prices & Ticker Parameters

**File:** `backend/app/market/seed_prices.py`

Constants used by the GBM simulator. Splitting these into a separate module makes
them easy to update without touching simulator logic, and they can also be imported
by the REST API to serve initial price estimates before the simulator has run.

```python
# backend/app/market/seed_prices.py

# Realistic starting prices for the default watchlist
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement per tick)
# mu:    annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM":  {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V":    {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR    = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR   = 0.3  # Between sectors / unknown tickers
TSLA_CORR          = 0.3  # TSLA does its own thing
```

**Volatility rationale:**
- TSLA `sigma=0.50` → ~50% annualized volatility, realistic for a high-beta growth stock
- V `sigma=0.17` → ~17% annualized volatility, realistic for a mature payments processor
- NVDA has the strongest drift (`mu=0.08`) reflecting recent GPU/AI tailwinds

**Tickers not in `SEED_PRICES`** (added dynamically) start at `random.uniform(50.0, 300.0)`.

---

## 7. GBM Simulator

**File:** `backend/app/market/simulator.py`

Two classes: `GBMSimulator` (the math engine) and `SimulatorDataSource` (the
`MarketDataSource` implementation that wraps it in an async loop).

### 7.1 GBM Math

Geometric Brownian Motion is the standard model for equity price paths. At each
time step, a price evolves as:

```
S(t+dt) = S(t) × exp((μ - σ²/2) × dt + σ × √dt × Z)
```

Where:
- `S(t)` — current price
- `μ` (mu) — annualized drift (expected return), e.g. 0.05 for 5%/year
- `σ` (sigma) — annualized volatility, e.g. 0.20 for 20%/year
- `dt` — time step as a fraction of a trading year
- `Z` — standard normal random variable drawn from N(0,1)

For 500ms ticks over a 252-day trading year with 6.5 hours/day:
```
dt = 0.5 / (252 × 6.5 × 3600) ≈ 8.48 × 10⁻⁸
```

This tiny `dt` produces sub-cent moves per tick that accumulate naturally into
realistic intraday price action over time.

### 7.2 Correlated Moves

Real stocks in the same sector move together. We use **Cholesky decomposition**
of a correlation matrix to produce correlated random draws:

```
Z_correlated = L @ Z_independent
```

Where `L` is the lower Cholesky factor of the correlation matrix `C`. Each
time tickers are added or removed, the matrix is rebuilt.

### 7.3 GBMSimulator

```python
# backend/app/market/simulator.py (GBMSimulator class)

import math, random, logging
import numpy as np
from .seed_prices import (
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS,
    INTRA_FINANCE_CORR, INTRA_TECH_CORR, SEED_PRICES,
    TICKER_PARAMS, TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        Hot path — called every 500ms.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to correlate them
        z = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]

            # GBM step: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift     = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick (~1 event/50s across 10 tickers)
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= (1 + shock)

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker; rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker; rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky — for batch initialization."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild Cholesky decomposition of the correlation matrix.

        Called on add/remove. O(n²) but n < 50, so negligible.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Sector-based pairwise correlation.

        tech × tech:     0.6
        finance × finance: 0.5
        TSLA × anything: 0.3
        cross-sector:    0.3
        unknown:         0.3
        """
        tech    = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech    and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### 7.4 SimulatorDataSource

```python
# backend/app/market/simulator.py (SimulatorDataSource class)

import asyncio
from .cache import PriceCache
from .interface import MarketDataSource


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task: step the simulator → write to cache → sleep.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)

        # Seed the cache immediately so SSE has data on the very first read
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step → write → sleep. Exceptions are caught and logged."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Behavior notes:**
- Prices never go negative — GBM is multiplicative (`exp()` is always positive)
- Random events fire ~0.1% of ticks. With 10 tickers at 2 ticks/second: roughly one notable move every 50 seconds
- When a ticker is added mid-session, the Cholesky matrix is rebuilt (~O(n²), negligible for n < 50)
- The initial cache seed in `start()` means the frontend gets real data on its first SSE connection with no delay

---

## 8. Massive API Client

**File:** `backend/app/market/massive_client.py`

Used when `MASSIVE_API_KEY` is set. Polls the Massive (Polygon.io) REST API for
all watched tickers in a single API call.

```python
# backend/app/market/massive_client.py

from __future__ import annotations
import asyncio, logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      Free tier:  5 req/min  → poll every 15s (default)
      Paid tiers: unlimited  → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll: cache has data before the interval fires
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Sleep then poll, forever. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # RESTClient is synchronous — run in thread pool to avoid blocking
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # API timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(
                        ticker=snap.ticker,
                        price=price,
                        timestamp=timestamp,
                    )
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"), e,
                    )
            logger.debug(
                "Massive poll: updated %d/%d tickers", processed, len(self._tickers)
            )

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — retry on next interval
            # Common failures: 401 bad key, 429 rate limit, network errors

    def _fetch_snapshots(self) -> list:
        """Synchronous API call. Runs in a thread via asyncio.to_thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Massive API Reference

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="your_key")   # or reads MASSIVE_API_KEY from env

# Snapshot all tickers — ONE API call for all watched tickers
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)

for snap in snapshots:
    ticker    = snap.ticker                          # "AAPL"
    price     = snap.last_trade.price               # 190.50
    timestamp = snap.last_trade.timestamp / 1000.0  # Unix ms → seconds
    prev_close= snap.day.previous_close             # previous day close
    day_pct   = snap.day.change_percent             # day % change

# Single ticker snapshot (more detail)
snap = client.get_snapshot_ticker(
    market_type=SnapshotMarketType.STOCKS,
    ticker="AAPL",
)
```

**Rate limits:**
- Free tier: 5 requests/minute → `poll_interval=15.0` (default)
- Paid tiers: unlimited → set `poll_interval=2.0` to `5.0`

**Error codes:**
- `401` — invalid API key
- `403` — plan doesn't include this endpoint
- `429` — rate limit exceeded
- `5xx` — server error (client has built-in retry, 3 attempts)

---

## 9. Factory

**File:** `backend/app/market/factory.py`

The factory reads the environment and returns the right implementation. The
rest of the application never imports `SimulatorDataSource` or
`MassiveDataSource` directly.

```python
# backend/app/market/factory.py

from __future__ import annotations
import logging, os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise                         → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

---

## 10. SSE Streaming Endpoint

**File:** `backend/app/market/stream.py`

Exposes `GET /api/stream/prices` as a Server-Sent Events stream. The frontend
connects with the native `EventSource` API and receives all ticker prices every
~500ms when the cache has changed.

```python
# backend/app/market/stream.py

from __future__ import annotations
import asyncio, json, logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with an injected PriceCache.

    Factory pattern avoids module-level globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint: streams all ticker prices every ~500ms.

        Client connects with:
            const es = new EventSource('/api/stream/prices');
            es.onmessage = (e) => {
                const prices = JSON.parse(e.data);
                // prices = { "AAPL": { ticker, price, previous_price, ... }, ... }
            };
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",   # Disable nginx buffering when proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Version-based change detection: only emits a new event when the cache
    version has advanced, avoiding sending duplicate payloads.
    """
    # Tell the browser to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

**SSE event format:**
```
retry: 1000

data: {"AAPL": {"ticker": "AAPL", "price": 190.50, "previous_price": 190.10, "timestamp": 1736000000.123, "change": 0.4, "change_percent": 0.2104, "direction": "up"}, "GOOGL": {...}, ...}

data: {"AAPL": {...}, "GOOGL": {...}, ...}

```

**Frontend consumption:**
```typescript
const es = new EventSource('/api/stream/prices');

es.onmessage = (event) => {
    const prices: Record<string, PriceUpdate> = JSON.parse(event.data);
    for (const [ticker, update] of Object.entries(prices)) {
        // Flash the cell green or red based on update.direction
        // Update sparkline history
        // Update watchlist row
    }
};

es.onerror = () => {
    // EventSource auto-reconnects after `retry: 1000` ms
    setConnectionStatus('reconnecting');
};
```

---

## 11. FastAPI Lifecycle Integration

The market data subsystem is wired into the FastAPI app via the `lifespan`
context manager. Here is the full integration pattern:

```python
# backend/app/main.py

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .market import PriceCache, create_market_data_source, create_stream_router

# Module-level singletons (initialized in lifespan)
price_cache: PriceCache | None = None
market_source = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown for background tasks."""
    global price_cache, market_source

    # --- Startup ---
    price_cache = PriceCache()
    market_source = create_market_data_source(price_cache)

    # Load the initial watchlist from the database
    initial_tickers = await db.get_watchlist_tickers(user_id="default")

    await market_source.start(initial_tickers)

    yield

    # --- Shutdown ---
    await market_source.stop()


app = FastAPI(lifespan=lifespan)

# Register SSE router (injected with the cache)
# Must be done at module level with a reference to the cache object.
# Since price_cache is not yet initialized at import time, we use a
# factory that captures it lazily via closure, or we register after
# the cache is created.
#
# Recommended pattern: initialize cache at module level as None,
# then use a dependency injection approach or capture in lifespan.

# Watchlist API routes (excerpt)
@app.post("/api/watchlist")
async def add_ticker(body: AddTickerRequest):
    ticker = body.ticker.upper().strip()
    await db.add_to_watchlist(ticker, user_id="default")
    await market_source.add_ticker(ticker)    # Tell the data source
    return {"ticker": ticker}

@app.delete("/api/watchlist/{ticker}")
async def remove_ticker(ticker: str):
    ticker = ticker.upper().strip()
    await db.remove_from_watchlist(ticker, user_id="default")
    await market_source.remove_ticker(ticker) # Removes from cache too
    return {"ticker": ticker}
```

**Dependency injection for price_cache in routes:**

```python
from fastapi import Depends

def get_price_cache() -> PriceCache:
    """FastAPI dependency: inject the global PriceCache."""
    return price_cache

@app.get("/api/watchlist")
async def get_watchlist(cache: PriceCache = Depends(get_price_cache)):
    tickers = await db.get_watchlist_tickers(user_id="default")
    result = []
    for ticker in tickers:
        update = cache.get(ticker)
        result.append({
            "ticker": ticker,
            "price": update.price if update else None,
            "change_percent": update.change_percent if update else None,
            "direction": update.direction if update else None,
        })
    return result

@app.post("/api/portfolio/trade")
async def execute_trade(
    body: TradeRequest,
    cache: PriceCache = Depends(get_price_cache),
):
    current_price = cache.get_price(body.ticker)
    if current_price is None:
        raise HTTPException(400, f"No price available for {body.ticker}")
    # ... rest of trade logic
```

---

## 12. Watchlist Coordination

When the user adds or removes a ticker via the REST API, **three things must happen**:

1. Database: insert/delete the `watchlist` row
2. Market source: `await market_source.add_ticker(ticker)` / `remove_ticker()`
3. Cache: automatically handled by `remove_ticker()` on the source

This ensures that newly added tickers start receiving price updates immediately,
and removed tickers disappear from SSE events on the next poll.

```python
# Adding a ticker — correct order
async def add_ticker_to_watchlist(ticker: str):
    # 1. Validate ticker exists (optional — the simulator will handle unknowns)
    # 2. Persist to database first (so on restart, the ticker is remembered)
    await db.add_to_watchlist(ticker, user_id="default")
    # 3. Tell the market source (starts generating prices for it)
    await market_source.add_ticker(ticker)
    # 4. Return — next SSE event will include the new ticker

# Removing a ticker — correct order
async def remove_ticker_from_watchlist(ticker: str):
    # 1. Remove from database
    await db.remove_from_watchlist(ticker, user_id="default")
    # 2. Tell the market source (also removes from PriceCache)
    await market_source.remove_ticker(ticker)
    # 3. Return — ticker disappears from next SSE event
```

**Simulator vs Massive behavior on add:**
- **Simulator:** `add_ticker()` immediately generates a seed price and writes it
  to the cache. The ticker appears in SSE output on the next event (within 500ms).
- **Massive:** `add_ticker()` appends to `_tickers` list. The price appears after
  the next poll cycle (up to 15 seconds on the free tier).

---

## 13. Public Package API

**File:** `backend/app/market/__init__.py`

Everything downstream needs is exported from this single import path:

```python
# backend/app/market/__init__.py

from .cache   import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models  import PriceUpdate
from .stream  import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

**All imports in downstream code use this path:**

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source
from app.market import create_stream_router
```

---

## 14. Testing Strategy

Tests live in `backend/tests/market/`. Run with:

```bash
cd backend
uv run --extra dev pytest tests/market/ -v
uv run --extra dev pytest tests/market/ --cov=app/market --cov-report=term-missing
```

### 14.1 test_models.py

```python
from app.market.models import PriceUpdate
import time

def test_direction_up():
    u = PriceUpdate(ticker="AAPL", price=101.0, previous_price=100.0)
    assert u.direction == "up"
    assert u.change == 1.0
    assert u.change_percent == pytest.approx(1.0, rel=1e-4)

def test_direction_down():
    u = PriceUpdate(ticker="AAPL", price=99.0, previous_price=100.0)
    assert u.direction == "down"
    assert u.change == -1.0

def test_direction_flat():
    u = PriceUpdate(ticker="AAPL", price=100.0, previous_price=100.0)
    assert u.direction == "flat"
    assert u.change == 0.0

def test_frozen():
    u = PriceUpdate(ticker="AAPL", price=100.0, previous_price=99.0)
    with pytest.raises((AttributeError, FrozenInstanceError)):
        u.price = 200.0

def test_to_dict():
    u = PriceUpdate(ticker="AAPL", price=190.50, previous_price=190.10)
    d = u.to_dict()
    assert d["ticker"] == "AAPL"
    assert d["price"] == 190.50
    assert d["direction"] == "up"
    assert "change_percent" in d
```

### 14.2 test_cache.py

```python
from app.market.cache import PriceCache

def test_update_first_time():
    cache = PriceCache()
    update = cache.update("AAPL", 190.0)
    assert update.price == 190.0
    assert update.previous_price == 190.0  # First update: prev == price
    assert update.direction == "flat"

def test_update_tracks_previous():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    update = cache.update("AAPL", 191.0)
    assert update.previous_price == 190.0
    assert update.direction == "up"

def test_version_increments():
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.0)
    assert cache.version == v0 + 1
    cache.update("AAPL", 191.0)
    assert cache.version == v0 + 2

def test_remove():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    assert "AAPL" in cache
    cache.remove("AAPL")
    assert "AAPL" not in cache
    assert cache.get("AAPL") is None

def test_get_all_is_copy():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    snapshot = cache.get_all()
    cache.update("AAPL", 191.0)
    # Snapshot is not affected by subsequent updates
    assert snapshot["AAPL"].price == 190.0
```

### 14.3 test_simulator.py

```python
from app.market.simulator import GBMSimulator

def test_step_returns_all_tickers():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    result = sim.step()
    assert set(result.keys()) == {"AAPL", "GOOGL"}
    assert all(isinstance(p, float) for p in result.values())

def test_prices_always_positive():
    sim = GBMSimulator(tickers=["AAPL", "TSLA"], event_probability=0.5)
    for _ in range(1000):
        prices = sim.step()
        assert all(p > 0 for p in prices.values())

def test_add_ticker():
    sim = GBMSimulator(tickers=["AAPL"])
    sim.add_ticker("GOOGL")
    assert "GOOGL" in sim.get_tickers()
    result = sim.step()
    assert "GOOGL" in result

def test_remove_ticker():
    sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
    sim.remove_ticker("GOOGL")
    assert "GOOGL" not in sim.get_tickers()
    result = sim.step()
    assert "GOOGL" not in result

def test_cholesky_10_tickers():
    """Correlation matrix must be positive definite for all 10 default tickers."""
    from app.market.seed_prices import SEED_PRICES
    sim = GBMSimulator(tickers=list(SEED_PRICES.keys()))
    # If Cholesky fails (non-PD matrix), this would raise LinAlgError
    result = sim.step()
    assert len(result) == 10
```

### 14.4 test_simulator_source.py (integration)

```python
import asyncio
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource

async def test_start_seeds_cache():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache)
    await source.start(["AAPL", "GOOGL"])

    # Cache should be populated immediately after start()
    assert cache.get("AAPL") is not None
    assert cache.get("GOOGL") is not None

    await source.stop()

async def test_prices_update_over_time():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
    await source.start(["AAPL"])

    v0 = cache.version
    await asyncio.sleep(0.3)
    assert cache.version > v0  # Cache should have updated

    await source.stop()

async def test_add_remove_ticker():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache)
    await source.start(["AAPL"])

    await source.add_ticker("TSLA")
    assert "TSLA" in source.get_tickers()
    assert cache.get("TSLA") is not None

    await source.remove_ticker("TSLA")
    assert "TSLA" not in source.get_tickers()
    assert cache.get("TSLA") is None

    await source.stop()
```

### 14.5 test_factory.py

```python
import os
from unittest.mock import patch
from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.simulator import SimulatorDataSource
from app.market.massive_client import MassiveDataSource

def test_no_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {}, clear=True):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)

def test_empty_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "  "}):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)

def test_api_key_set_returns_massive():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key-123"}):
        source = create_market_data_source(cache)
    assert isinstance(source, MassiveDataSource)
```

### 14.6 test_massive.py

```python
from unittest.mock import MagicMock, patch, AsyncMock
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource

def make_snapshot(ticker: str, price: float, timestamp_ms: int = 1700000000000):
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap

async def test_poll_updates_cache():
    cache = PriceCache()
    source = MassiveDataSource(api_key="fake", price_cache=cache)
    source._client = MagicMock()

    snapshots = [make_snapshot("AAPL", 190.0), make_snapshot("GOOGL", 175.0)]
    with patch.object(source, "_fetch_snapshots", return_value=snapshots):
        await source._poll_once()

    assert cache.get_price("AAPL") == 190.0
    assert cache.get_price("GOOGL") == 175.0

async def test_malformed_snapshot_skipped():
    cache = PriceCache()
    source = MassiveDataSource(api_key="fake", price_cache=cache)
    source._client = MagicMock()

    bad_snap = MagicMock()
    bad_snap.ticker = "BAD"
    bad_snap.last_trade = None   # Will raise AttributeError

    good_snap = make_snapshot("AAPL", 190.0)

    with patch.object(source, "_fetch_snapshots", return_value=[bad_snap, good_snap]):
        await source._poll_once()   # Should not raise

    assert cache.get_price("AAPL") == 190.0  # Good one processed
    assert cache.get_price("BAD") is None    # Bad one skipped
```

---

## 15. Error Handling & Edge Cases

### Simulator

| Situation | Behavior |
|---|---|
| Empty ticker list | `step()` returns `{}` immediately; no crash |
| Unknown ticker added | Seeds from `random.uniform(50, 300)`; uses `DEFAULT_PARAMS` |
| Cholesky fails (bad corr matrix) | Should not happen with the defined correlation values (all positive definite); if it did, `LinAlgError` would propagate up |
| Step raises an exception | `_run_loop` catches and logs via `logger.exception()`; continues on the next tick |
| Stop called before start | `_task` is `None`; no-op |

### Massive API

| Situation | Behavior |
|---|---|
| Invalid API key (401) | `_poll_once` catches and logs `logger.error()`; retries on next interval |
| Rate limited (429) | Same — logged, retried. Adjust `poll_interval` to avoid |
| Malformed snapshot | Per-snapshot try/except skips the bad entry, logs a warning, continues |
| Network timeout | `Exception` caught in `_poll_once`; retried on next interval |
| Ticker added but not yet polled | It appears in `_tickers` list; the next poll includes it (up to 15s delay on free tier) |
| Market closed | `last_trade.price` reflects the most recent traded price (after-hours / last close) |

### SSE Stream

| Situation | Behavior |
|---|---|
| Client disconnects | `request.is_disconnected()` returns `True`; generator breaks and cleans up |
| Cache is empty | No `data:` event sent; client waits for next tick |
| Cache unchanged | `version` check prevents sending duplicate events |
| Connection dropped unexpectedly | `retry: 1000` directive makes browser reconnect after 1 second |
| `asyncio.CancelledError` | Caught and logged; generator exits cleanly |

---

## 16. Configuration Summary

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | (unset) | If set and non-empty: use Massive API. Otherwise: use simulator |
| Simulator `update_interval` | `0.5` seconds | How often `GBMSimulator.step()` is called |
| Simulator `event_probability` | `0.001` (0.1%) | Chance per tick per ticker of a random 2-5% shock |
| GBMSimulator `dt` | `~8.48e-8` | Time step as fraction of trading year (500ms ticks) |
| Massive `poll_interval` | `15.0` seconds | Seconds between REST polls (15s = safe for free tier) |
| SSE `interval` | `0.5` seconds | How often `_generate_events` checks the cache |

**Quick-start for downstream developers:**

```python
from app.market import PriceCache, create_market_data_source, create_stream_router

# 1. Create cache and source (reads MASSIVE_API_KEY from env)
cache  = PriceCache()
source = create_market_data_source(cache)

# 2. Start with initial tickers (from DB watchlist)
await source.start(["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                    "NVDA", "META", "JPM", "V", "NFLX"])

# 3. Read prices anywhere
update = cache.get("AAPL")          # PriceUpdate | None
price  = cache.get_price("AAPL")    # float | None
all_px = cache.get_all()            # dict[str, PriceUpdate]

# 4. Dynamic watchlist management
await source.add_ticker("PYPL")     # Starts generating prices
await source.remove_ticker("NFLX")  # Stops prices, removes from cache

# 5. Register SSE endpoint with FastAPI
stream_router = create_stream_router(cache)
app.include_router(stream_router)
# → GET /api/stream/prices is now live

# 6. Shutdown cleanly
await source.stop()
```
