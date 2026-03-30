"""
Investment & market data.
Sources:
  - Yahoo Finance  (no key, global coverage)
  - Twelve Data    (optional API key, enriches with fundamentals + history)
  - KASE           (fallback for Kazakh companies)
"""
import asyncio
import logging
from typing import Any

import httpx

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
# Yahoo Finance
# ──────────────────────────────────────────────

async def _yahoo_search(company_name: str) -> str | None:
    """Return best-matching Yahoo Finance symbol for company name."""
    try:
        r = await _http.get(
            "https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": company_name, "quotesCount": 5, "newsCount": 0, "enableFuzzyQuery": True},
        )
        r.raise_for_status()
        quotes = r.json().get("quotes", [])
        # Prefer equity type
        for q in quotes:
            if q.get("quoteType") in ("EQUITY", "ETF"):
                return q.get("symbol")
        return quotes[0].get("symbol") if quotes else None
    except Exception as e:
        logger.debug("Yahoo search failed for '%s': %s", company_name, e)
        return None


async def _yahoo_quote(symbol: str) -> dict[str, Any] | None:
    """Fetch detailed quote from Yahoo Finance."""
    try:
        r = await _http.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": symbol, "fields": ",".join([
                "shortName", "regularMarketPrice", "regularMarketChange",
                "regularMarketChangePercent", "regularMarketVolume",
                "marketCap", "trailingPE", "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
                "regularMarketDayHigh", "regularMarketDayLow",
                "sector", "industry", "currency", "fullExchangeName",
                "averageDailyVolume10Day",
            ])},
        )
        r.raise_for_status()
        result = r.json().get("quoteResponse", {}).get("result", [])
        if not result:
            return None
        q = result[0]
        return {
            "ticker":        q.get("symbol"),
            "name":          q.get("shortName", symbol),
            "exchange":      q.get("fullExchangeName", ""),
            "currency":      q.get("currency", ""),
            "last":          q.get("regularMarketPrice"),
            "change":        q.get("regularMarketChange"),
            "change_pct":    q.get("regularMarketChangePercent"),
            "volume":        q.get("regularMarketVolume"),
            "avg_volume":    q.get("averageDailyVolume10Day"),
            "market_cap":    q.get("marketCap"),
            "pe_ratio":      q.get("trailingPE"),
            "week52_high":   q.get("fiftyTwoWeekHigh"),
            "week52_low":    q.get("fiftyTwoWeekLow"),
            "day_high":      q.get("regularMarketDayHigh"),
            "day_low":       q.get("regularMarketDayLow"),
            "sector":        q.get("sector", ""),
            "industry":      q.get("industry", ""),
            "source":        "Yahoo Finance",
        }
    except Exception as e:
        logger.debug("Yahoo quote failed for '%s': %s", symbol, e)
        return None


async def _yahoo_peers(symbol: str) -> list[dict[str, Any]]:
    """Fetch recommended peer stocks from Yahoo Finance."""
    try:
        r = await _http.get(
            f"https://query2.finance.yahoo.com/v6/finance/recommendationsbysymbol/{symbol}",
        )
        r.raise_for_status()
        data = r.json()
        rec_list = (data.get("finance", {}).get("result") or [{}])[0].get("recommendedSymbols", [])
        symbols = [s.get("symbol") for s in rec_list[:8] if s.get("symbol")]
        if not symbols:
            return []
        # Bulk quote for peers
        r2 = await _http.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": ",".join(symbols)},
        )
        r2.raise_for_status()
        quotes = r2.json().get("quoteResponse", {}).get("result", [])
        peers = []
        for q in quotes:
            peers.append({
                "ticker":     q.get("symbol"),
                "name":       q.get("shortName", q.get("symbol")),
                "last":       q.get("regularMarketPrice"),
                "change_pct": q.get("regularMarketChangePercent"),
                "volume":     q.get("regularMarketVolume"),
                "market_cap": q.get("marketCap"),
                "exchange":   q.get("fullExchangeName", ""),
            })
        return peers
    except Exception as e:
        logger.debug("Yahoo peers failed for '%s': %s", symbol, e)
        return []


async def _yahoo_trending(region: str = "US") -> list[dict[str, Any]]:
    """Fetch trending tickers from Yahoo Finance for market context."""
    try:
        r = await _http.get(
            f"https://query1.finance.yahoo.com/v1/finance/trending/{region}",
            params={"count": 10},
        )
        r.raise_for_status()
        quotes = (r.json().get("finance", {}).get("result") or [{}])[0].get("quotes", [])
        symbols = [q.get("symbol") for q in quotes if q.get("symbol")][:10]
        if not symbols:
            return []
        r2 = await _http.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": ",".join(symbols)},
        )
        r2.raise_for_status()
        result = []
        for q in r2.json().get("quoteResponse", {}).get("result", []):
            result.append({
                "ticker":     q.get("symbol"),
                "name":       q.get("shortName", q.get("symbol")),
                "last":       q.get("regularMarketPrice"),
                "change_pct": q.get("regularMarketChangePercent"),
                "volume":     q.get("regularMarketVolume"),
                "market_cap": q.get("marketCap"),
                "exchange":   q.get("fullExchangeName", ""),
            })
        return result
    except Exception as e:
        logger.debug("Yahoo trending failed: %s", e)
        return []


# ──────────────────────────────────────────────
# Twelve Data
# ──────────────────────────────────────────────

async def _twelve_quote(symbol: str, api_key: str) -> dict[str, Any] | None:
    """Fetch real-time quote from Twelve Data."""
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
            "open":          d.get("open"),
            "prev_close":    d.get("previous_close"),
            "week52_high":   d.get("fifty_two_week", {}).get("high"),
            "week52_low":    d.get("fifty_two_week", {}).get("low"),
            "avg_volume":    d.get("average_volume"),
            "exchange":      d.get("exchange"),
            "source_td":     "Twelve Data",
        }
    except Exception as e:
        logger.debug("Twelve Data quote failed for '%s': %s", symbol, e)
        return None


async def _twelve_time_series(symbol: str, api_key: str, days: int = 30) -> list[dict[str, Any]]:
    """Fetch historical OHLCV data from Twelve Data."""
    try:
        r = await _http.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol":     symbol,
                "interval":   "1day",
                "outputsize": days,
                "apikey":     api_key,
            },
        )
        r.raise_for_status()
        d = r.json()
        if d.get("status") == "error":
            return []
        values = d.get("values", [])
        return [
            {
                "date":   v.get("datetime"),
                "open":   float(v["open"])   if v.get("open")   else None,
                "high":   float(v["high"])   if v.get("high")   else None,
                "low":    float(v["low"])    if v.get("low")    else None,
                "close":  float(v["close"])  if v.get("close")  else None,
                "volume": int(v["volume"])   if v.get("volume") else None,
            }
            for v in reversed(values)
        ]
    except Exception as e:
        logger.debug("Twelve Data time series failed for '%s': %s", symbol, e)
        return []


async def _twelve_top_gainers_losers(api_key: str) -> dict[str, list]:
    """Fetch market gainers and losers from Twelve Data."""
    try:
        r = await _http.get(
            "https://api.twelvedata.com/mutual_funds/list",
            params={"apikey": api_key, "outputsize": 5},
        )
        # Gainers/losers endpoint (use market movers)
        r2 = await _http.get(
            "https://api.twelvedata.com/market_movers/stocks",
            params={"apikey": api_key, "direction": "gainers", "country": "United States"},
        )
        r2.raise_for_status()
        d = r2.json()
        gainers = [
            {
                "ticker":     s.get("ticker_id"),
                "name":       s.get("name"),
                "last":       s.get("last_price"),
                "change_pct": s.get("percent_change"),
                "volume":     s.get("volume"),
            }
            for s in (d.get("values") or [])[:6]
        ]
        r3 = await _http.get(
            "https://api.twelvedata.com/market_movers/stocks",
            params={"apikey": api_key, "direction": "losers", "country": "United States"},
        )
        r3.raise_for_status()
        d3 = r3.json()
        losers = [
            {
                "ticker":     s.get("ticker_id"),
                "name":       s.get("name"),
                "last":       s.get("last_price"),
                "change_pct": s.get("percent_change"),
                "volume":     s.get("volume"),
            }
            for s in (d3.get("values") or [])[:6]
        ]
        return {"gainers": gainers, "losers": losers}
    except Exception as e:
        logger.debug("Twelve Data market movers failed: %s", e)
        return {"gainers": [], "losers": []}


# ──────────────────────────────────────────────
# KASE fallback
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
    Comprehensive market data combining Yahoo Finance + Twelve Data.
    Returns stock quote, historical series, peers, market movers.
    """
    # 1. Find symbol on Yahoo Finance
    symbol = await _yahoo_search(company_name)

    # 2. Parallel: Yahoo quote + KASE fallback + trending
    yahoo_quote, kase_quote, trending = await asyncio.gather(
        _yahoo_quote(symbol) if symbol else asyncio.sleep(0, result=None),
        _fetch_kase_data(company_name),
        _yahoo_trending("US"),
    )

    stock: dict[str, Any] | None = yahoo_quote or kase_quote

    # 3. If found on Yahoo — get peers + Twelve Data enrichment
    peers: list[dict] = []
    time_series: list[dict] = []
    td_extra: dict = {}
    market_movers: dict = {"gainers": [], "losers": []}

    if symbol:
        tasks = [_yahoo_peers(symbol)]
        if twelve_key:
            tasks += [
                _twelve_quote(symbol, twelve_key),
                _twelve_time_series(symbol, twelve_key, days=60),
                _twelve_top_gainers_losers(twelve_key),
            ]
        else:
            tasks += [
                asyncio.sleep(0, result=None),
                asyncio.sleep(0, result=[]),
                asyncio.sleep(0, result={"gainers": [], "losers": []}),
            ]

        results = await asyncio.gather(*tasks)
        peers       = results[0]
        td_extra    = results[1] or {}
        time_series = results[2]
        market_movers = results[3]

        # Merge Twelve Data into stock dict
        if stock and td_extra:
            for k, v in td_extra.items():
                if v is not None and k not in stock:
                    stock[k] = v
            if "source_td" in td_extra:
                stock["source"] = f"Yahoo Finance + Twelve Data"

    return {
        "stock":         stock,
        "found":         stock is not None,
        "symbol":        symbol,
        "peers":         peers,
        "time_series":   time_series,
        "trending":      trending,
        "market_movers": market_movers,
    }
