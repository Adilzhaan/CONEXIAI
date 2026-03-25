import httpx
from typing import Any
from urllib.parse import quote

_http = httpx.AsyncClient(timeout=10, follow_redirects=True)


async def close() -> None:
    await _http.aclose()


async def fetch_vacancies(company_name: str, limit: int = 10) -> list[dict[str, Any]]:
    """Fetch recent job vacancies for a company from HH.ru public API."""
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": company_name,
        "employer_name": company_name,
        "per_page": limit,
        "order_by": "publication_time",
    }
    try:
        r = await _http.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        vacancies = []
        for v in data.get("items", []):
            salary = v.get("salary")
            salary_str = ""
            if salary:
                parts = []
                if salary.get("from"):
                    parts.append(f"от {salary['from']}")
                if salary.get("to"):
                    parts.append(f"до {salary['to']}")
                currency = salary.get("currency", "")
                salary_str = " ".join(parts) + (f" {currency}" if currency else "")
            vacancies.append({
                "title": v.get("name", ""),
                "employer": v.get("employer", {}).get("name", ""),
                "area": v.get("area", {}).get("name", ""),
                "salary": salary_str,
                "url": v.get("alternate_url", ""),
                "published_at": (v.get("published_at") or "")[:10],
            })
        return vacancies
    except Exception:
        return []
