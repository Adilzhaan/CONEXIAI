"""
Investment & market data.
Sources:
  - yfinance  (Yahoo Finance wrapper — NYSE, NASDAQ, LSE, KASE, AIX)
  - Twelve Data    (optional API key, enriches with fundamentals + history)
  - KASE REST API  (fallback for Kazakh companies)
"""
import asyncio
import logging
from typing import Any

import httpx
import yfinance as yf

logger = logging.getLogger("conexiai")

_http = httpx.AsyncClient(
    timeout=15,
    follow_redirects=True,
    headers={
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json",
    },
)


async def close() -> None:
    await _http.aclose()


# ──────────────────────────────────────────────
# yfinance helpers  (sync → async via thread)
# ──────────────────────────────────────────────

_PREFERRED_EXCHANGES = {"KAZ", "AIX", "NYQ", "NMS", "NGM", "LSE", "FRA", "AMS"}


def _yf_search_sync(company_name: str) -> str | None:
    """Find best ticker for a company name using yfinance Search."""
    try:
        results = yf.Search(company_name, max_results=8, news_count=0).quotes
        # 1st pass: preferred exchanges
        for q in results:
            if q.get("quoteType") in ("EQUITY", "ETF") and q.get("exchange") in _PREFERRED_EXCHANGES:
                return q.get("symbol")
        # 2nd pass: any equity
        for q in results:
            if q.get("quoteType") in ("EQUITY", "ETF"):
                return q.get("symbol")
        return results[0].get("symbol") if results else None
    except Exception as e:
        logger.debug("yfinance search failed for '%s': %s", company_name, e)
        return None


def _yf_quote_sync(symbol: str) -> dict[str, Any] | None:
    """Fetch quote for a single ticker using yfinance fast_info."""
    try:
        ticker = yf.Ticker(symbol)
        fi = ticker.fast_info
        price = getattr(fi, "last_price", None)
        if not price:
            return None
        prev = getattr(fi, "previous_close", None)
        change_pct = round(((price - prev) / prev) * 100, 2) if prev else None
        change = round(price - prev, 4) if prev else None
        return {
            "ticker":      symbol,
            "name":        getattr(fi, "long_name", None) or symbol,
            "exchange":    getattr(fi, "exchange", ""),
            "currency":    getattr(fi, "currency", ""),
            "last":        round(price, 4),
            "change":      change,
            "change_pct":  change_pct,
            "volume":      getattr(fi, "three_month_average_volume", None),
            "market_cap":  getattr(fi, "market_cap", None),
            "week52_high": getattr(fi, "year_high", None),
            "week52_low":  getattr(fi, "year_low", None),
            "source":      "Yahoo Finance",
        }
    except Exception as e:
        logger.debug("yfinance quote failed for '%s': %s", symbol, e)
        return None


def _yf_multi_quote_sync(symbols: list[str]) -> list[dict[str, Any]]:
    """Fetch multiple tickers at once using yfinance Tickers."""
    if not symbols:
        return []
    try:
        tickers = yf.Tickers(" ".join(symbols))
        results = []
        for sym, ticker in tickers.tickers.items():
            fi = ticker.fast_info
            price = getattr(fi, "last_price", None)
            if not price:
                continue
            prev = getattr(fi, "previous_close", None)
            change_pct = round(((price - prev) / prev) * 100, 2) if prev else None
            results.append({
                "ticker":      sym,
                "name":        getattr(fi, "long_name", None) or sym,
                "exchange":    getattr(fi, "exchange", ""),
                "currency":    getattr(fi, "currency", ""),
                "last":        round(price, 4),
                "change":      round(price - prev, 4) if prev else None,
                "change_pct":  change_pct,
                "volume":      getattr(fi, "three_month_average_volume", None),
                "market_cap":  getattr(fi, "market_cap", None),
                "source":      "Yahoo Finance",
            })
        return results
    except Exception as e:
        logger.debug("yfinance multi-quote failed: %s", e)
        return []


def _yf_history_sync(symbol: str, days: int = 60) -> list[dict[str, Any]]:
    """Fetch daily OHLCV history via yfinance."""
    try:
        hist = yf.Ticker(symbol).history(period=f"{days}d")
        if hist.empty:
            return []
        return [
            {
                "date":   str(idx.date()),
                "open":   round(float(row["Open"]), 2),
                "high":   round(float(row["High"]), 2),
                "low":    round(float(row["Low"]), 2),
                "close":  round(float(row["Close"]), 2),
                "volume": int(row["Volume"]) if row["Volume"] else 0,
            }
            for idx, row in hist.iterrows()
        ]
    except Exception as e:
        logger.debug("yfinance history failed for '%s': %s", symbol, e)
        return []


async def _yahoo_search(company_name: str) -> str | None:
    return await asyncio.to_thread(_yf_search_sync, company_name)


async def _yahoo_quote(symbol: str) -> dict[str, Any] | None:
    return await asyncio.to_thread(_yf_quote_sync, symbol)


async def _yahoo_multi(symbols: list[str]) -> list[dict[str, Any]]:
    return await asyncio.to_thread(_yf_multi_quote_sync, symbols)


async def _yahoo_peers(symbol: str) -> list[dict[str, Any]]:
    """Fetch recommended peers via Yahoo Finance recommendations endpoint."""
    try:
        r = await _http.get(
            f"https://query2.finance.yahoo.com/v6/finance/recommendationsbysymbol/{symbol}",
        )
        r.raise_for_status()
        data = r.json()
        rec_list = (data.get("finance", {}).get("result") or [{}])[0].get("recommendedSymbols", [])
        syms = [s.get("symbol") for s in rec_list[:8] if s.get("symbol")]
        if not syms:
            return []
        return await _yahoo_multi(syms)
    except Exception as e:
        logger.debug("Yahoo peers failed for '%s': %s", symbol, e)
        return []


async def _yahoo_trending(region: str = "US") -> list[dict[str, Any]]:
    """Fetch trending tickers from Yahoo Finance."""
    try:
        r = await _http.get(
            f"https://query1.finance.yahoo.com/v1/finance/trending/{region}",
            params={"count": 10},
        )
        r.raise_for_status()
        quotes = (r.json().get("finance", {}).get("result") or [{}])[0].get("quotes", [])
        syms = [q.get("symbol") for q in quotes if q.get("symbol")][:10]
        if not syms:
            return []
        return await _yahoo_multi(syms)
    except Exception as e:
        logger.debug("Yahoo trending failed: %s", e)
        return []


# ──────────────────────────────────────────────
# Market indices  (S&P 500 / NASDAQ / FTSE / KASE)
# ──────────────────────────────────────────────

_GLOBAL_INDICES: dict[str, str] = {
    "S&P 500":  "^GSPC",
    "NASDAQ":   "^IXIC",
    "FTSE 100": "^FTSE",
}


def _yf_indices_history_sync(days: int = 60) -> dict[str, list[dict[str, Any]]]:
    """Fetch daily close history for all global indices."""
    out: dict[str, list] = {}
    for label, sym in _GLOBAL_INDICES.items():
        try:
            hist = yf.Ticker(sym).history(period=f"{days}d")
            if hist.empty:
                continue
            out[label] = [
                {"date": str(idx.date()), "close": round(float(row["Close"]), 2)}
                for idx, row in hist.iterrows()
            ]
        except Exception as e:
            logger.debug("yfinance index history failed for '%s': %s", sym, e)
    return out


async def _fetch_global_indices() -> list[dict[str, Any]]:
    """Fetch S&P 500, NASDAQ, FTSE 100 via yfinance."""
    name_map = {v: k for k, v in _GLOBAL_INDICES.items()}

    def _sync():
        out = []
        for sym, label in _GLOBAL_INDICES.items():
            try:
                fi = yf.Ticker(label).fast_info
                price = getattr(fi, "last_price", None)
                if not price:
                    continue
                prev = getattr(fi, "previous_close", None)
                chg = round(((price - prev) / prev) * 100, 2) if prev else None
                out.append({
                    "name":       sym,
                    "symbol":     label,
                    "last":       round(price, 2),
                    "change_pct": chg,
                    "exchange":   getattr(fi, "exchange", ""),
                })
            except Exception:
                pass
        return out

    return await asyncio.to_thread(_sync)


async def _fetch_kase_index() -> dict[str, Any] | None:
    """Fetch KASE composite index from the KASE public API."""
    try:
        r = await _http.get("https://api.kase.kz/api/indices", timeout=8)
        r.raise_for_status()
        items = r.json()
        if not items:
            return None
        item = items[0]
        return {
            "name":       item.get("name") or "KASE Index",
            "symbol":     item.get("code", "KASE"),
            "last":       item.get("lastValue") or item.get("value"),
            "change_pct": item.get("change"),
            "exchange":   "KASE",
        }
    except Exception as e:
        logger.debug("KASE index fetch failed: %s", e)
        return None


# ──────────────────────────────────────────────
# KASE company fallback
# ──────────────────────────────────────────────

async def _fetch_kase_data(company_name: str) -> dict[str, Any] | None:
    try:
        r = await _http.get(
            "https://api.kase.kz/api/securities/search",
            params={"query": company_name, "limit": 3},
        )
        r.raise_for_status()
        items = r.json()
        if not items:
            return None
        item = items[0]
        return {
            "ticker":     item.get("code") or item.get("ticker", ""),
            "name":       item.get("name", company_name),
            "last":       item.get("lastPrice"),
            "change_pct": item.get("change"),
            "source":     "KASE",
            "exchange":   "KASE",
            "currency":   "KZT",
        }
    except Exception as e:
        logger.debug("KASE fetch failed: %s", e)
        return None


# ──────────────────────────────────────────────
# Twelve Data  (optional enrichment)
# ──────────────────────────────────────────────

async def _twelve_quote(symbol: str, api_key: str) -> dict[str, Any] | None:
    try:
        r = await _http.get(
            "https://api.twelvedata.com/quote",
            params={"symbol": symbol, "apikey": api_key},
        )
        r.raise_for_status()
        d = r.json()
        if d.get("status") == "error" or "code" in d:
            return None
        return {
            "open":        d.get("open"),
            "prev_close":  d.get("previous_close"),
            "week52_high": d.get("fifty_two_week", {}).get("high"),
            "week52_low":  d.get("fifty_two_week", {}).get("low"),
            "avg_volume":  d.get("average_volume"),
            "exchange":    d.get("exchange"),
            "source_td":   "Twelve Data",
        }
    except Exception as e:
        logger.debug("Twelve Data quote failed for '%s': %s", symbol, e)
        return None


async def _twelve_time_series(symbol: str, api_key: str, days: int = 30) -> list[dict[str, Any]]:
    try:
        r = await _http.get(
            "https://api.twelvedata.com/time_series",
            params={"symbol": symbol, "interval": "1day", "outputsize": days, "apikey": api_key},
        )
        r.raise_for_status()
        d = r.json()
        if d.get("status") == "error":
            return []
        return [
            {
                "date":   v.get("datetime"),
                "open":   float(v["open"])  if v.get("open")  else None,
                "high":   float(v["high"])  if v.get("high")  else None,
                "low":    float(v["low"])   if v.get("low")   else None,
                "close":  float(v["close"]) if v.get("close") else None,
                "volume": int(v["volume"])  if v.get("volume") else None,
            }
            for v in reversed(d.get("values", []))
        ]
    except Exception as e:
        logger.debug("Twelve Data time series failed for '%s': %s", symbol, e)
        return []


async def _twelve_top_gainers_losers(api_key: str) -> dict[str, list]:
    try:
        r2 = await _http.get(
            "https://api.twelvedata.com/market_movers/stocks",
            params={"apikey": api_key, "direction": "gainers", "country": "United States"},
        )
        r2.raise_for_status()
        gainers = [
            {"ticker": s.get("ticker_id"), "name": s.get("name"),
             "last": s.get("last_price"), "change_pct": s.get("percent_change")}
            for s in (r2.json().get("values") or [])[:6]
        ]
        r3 = await _http.get(
            "https://api.twelvedata.com/market_movers/stocks",
            params={"apikey": api_key, "direction": "losers", "country": "United States"},
        )
        r3.raise_for_status()
        losers = [
            {"ticker": s.get("ticker_id"), "name": s.get("name"),
             "last": s.get("last_price"), "change_pct": s.get("percent_change")}
            for s in (r3.json().get("values") or [])[:6]
        ]
        return {"gainers": gainers, "losers": losers}
    except Exception as e:
        logger.debug("Twelve Data market movers failed: %s", e)
        return {"gainers": [], "losers": []}


# ──────────────────────────────────────────────
# Public interface
# ──────────────────────────────────────────────

async def fetch_market_data(company_name: str) -> dict[str, Any]:
    """Light version used on the company overview card."""
    symbol = await _yahoo_search(company_name)
    stock = await _yahoo_quote(symbol) if symbol else None
    if not stock:
        stock = await _fetch_kase_data(company_name)
    return {"stock": stock, "found": stock is not None}


async def fetch_full_market_data(
    company_name: str,
    twelve_key: str = "",
) -> dict[str, Any]:
    """
    Comprehensive market data: stock quote, global indices, peers, trending.
    Sources: yfinance (Yahoo Finance) + KASE API + optional Twelve Data.
    """
    # 1. Find ticker symbol
    symbol = await _yahoo_search(company_name)

    # 2. Parallel fetch: stock quote + KASE fallback + global indices + KASE index + indices history
    yahoo_quote, kase_quote, global_indices, kase_index, indices_history = await asyncio.gather(
        _yahoo_quote(symbol) if symbol else asyncio.sleep(0, result=None),
        _fetch_kase_data(company_name),
        _fetch_global_indices(),
        _fetch_kase_index(),
        asyncio.to_thread(_yf_indices_history_sync, 60),
    )

    stock: dict[str, Any] | None = yahoo_quote or kase_quote

    # KASE index first, then global (Kazakhstan focus)
    market_indices: list[dict] = []
    if kase_index:
        market_indices.append(kase_index)
    market_indices.extend(global_indices)

    # 3. Peers + trending + optional Twelve Data enrichment
    peers: list[dict] = []
    time_series: list[dict] = []
    td_extra: dict = {}
    market_movers: dict = {"gainers": [], "losers": []}
    top_stocks: list[dict] = []

    if symbol:
        tasks = [
            _yahoo_peers(symbol),
            _yahoo_trending("US"),
            asyncio.to_thread(_yf_history_sync, symbol, 60),
        ]
        if twelve_key:
            tasks += [
                _twelve_quote(symbol, twelve_key),
                _twelve_top_gainers_losers(twelve_key),
            ]
        else:
            tasks += [
                asyncio.sleep(0, result=None),
                asyncio.sleep(0, result={"gainers": [], "losers": []}),
            ]

        results = await asyncio.gather(*tasks)
        peers         = results[0]
        top_stocks    = results[1]
        time_series   = results[2]
        td_extra      = results[3] or {}
        market_movers = results[4]

        if stock and td_extra:
            for k, v in td_extra.items():
                if v is not None and k not in stock:
                    stock[k] = v
            if "source_td" in td_extra:
                stock["source"] = "Yahoo Finance + Twelve Data"
    else:
        top_stocks = await _yahoo_trending("US")

    return {
        "stock":           stock,
        "found":           stock is not None,
        "symbol":          symbol,
        "peers":           peers,
        "price_history":   time_series,
        "indices_history": indices_history,
        "top_stocks":      top_stocks,
        "market_indices":  market_indices,
        "market_movers":   market_movers,
    }
