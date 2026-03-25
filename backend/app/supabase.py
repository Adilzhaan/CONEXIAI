import httpx
from typing import Any

from .config import settings


class SupabaseClient:
    def __init__(self) -> None:
        self._base = settings.SUPABASE_URL.rstrip("/")
        self._anon_key = settings.SUPABASE_ANON_KEY
        self._http = httpx.AsyncClient(timeout=30)

    async def close(self) -> None:
        await self._http.aclose()

    def _headers(self, access_token: str | None = None) -> dict[str, str]:
        headers = {
            "apikey": self._anon_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        return headers

    async def auth_sign_up(self, email: str, password: str) -> dict[str, Any]:
        # https://supabase.com/docs/reference/auth-api/sign-up
        url = f"{self._base}/auth/v1/signup"
        payload = {"email": email, "password": password}
        r = await self._http.post(url, headers=self._headers(), json=payload)
        r.raise_for_status()
        return r.json()

    async def auth_sign_in_password(self, email: str, password: str) -> dict[str, Any]:
        # https://supabase.com/docs/reference/auth-api/sign-in-with-password
        url = f"{self._base}/auth/v1/token?grant_type=password"
        payload = {"email": email, "password": password}
        r = await self._http.post(url, headers=self._headers(), json=payload)
        r.raise_for_status()
        return r.json()

    async def auth_refresh(self, refresh_token: str) -> dict[str, Any]:
        url = f"{self._base}/auth/v1/token?grant_type=refresh_token"
        payload = {"refresh_token": refresh_token}
        r = await self._http.post(url, headers=self._headers(), json=payload)
        r.raise_for_status()
        return r.json()

    async def auth_get_user(self, access_token: str) -> dict[str, Any]:
        url = f"{self._base}/auth/v1/user"
        r = await self._http.get(url, headers=self._headers(access_token))
        r.raise_for_status()
        return r.json()

    async def rest_select(
        self,
        table: str,
        access_token: str,
        select: str = "*",
        order_by: str | None = None,
        limit: int | None = None,
        # Example: query_params={"company_id": "eq.<uuid>"}
        query_params: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        url = f"{self._base}/rest/v1/{table}"
        params: dict[str, Any] = {"select": select}
        if order_by:
            params["order"] = order_by
        if limit is not None:
            params["limit"] = str(limit)
        if query_params:
            params.update(query_params)

        r = await self._http.get(url, headers=self._headers(access_token), params=params)
        r.raise_for_status()
        return r.json()

    async def rest_raw_get(self, path_with_query: str, access_token: str) -> list[dict[str, Any]]:
        # Example: rest/v1/companies?select=id,name&order=created_at.desc
        url = f"{self._base}/{path_with_query.lstrip('/')}"
        r = await self._http.get(url, headers=self._headers(access_token))
        r.raise_for_status()
        return r.json()

    async def rest_insert(
        self,
        table: str,
        access_token: str,
        row: dict[str, Any],
        *,
        returning: str = "representation",
    ) -> dict[str, Any]:
        url = f"{self._base}/rest/v1/{table}"
        headers = self._headers(access_token)
        headers["Prefer"] = f"return={returning}"
        r = await self._http.post(url, headers=headers, json=[row])
        r.raise_for_status()
        data = r.json()
        if not data:
            raise RuntimeError("Supabase insert returned empty response.")
        return data[0]

    async def rest_insert_many(
        self,
        table: str,
        access_token: str,
        rows: list[dict[str, Any]],
        *,
        returning: str = "representation",
    ) -> list[dict[str, Any]]:
        url = f"{self._base}/rest/v1/{table}"
        headers = self._headers(access_token)
        headers["Prefer"] = f"return={returning}"
        r = await self._http.post(url, headers=headers, json=rows)
        r.raise_for_status()
        return r.json()

    async def rest_update_raw(
        self,
        path_with_query: str,
        access_token: str,
        patch: dict[str, Any],
        *,
        returning: str = "representation",
    ) -> list[dict[str, Any]]:
        url = f"{self._base}/{path_with_query.lstrip('/')}"
        headers = self._headers(access_token)
        headers["Prefer"] = f"return={returning}"
        r = await self._http.patch(url, headers=headers, json=patch)
        r.raise_for_status()
        return r.json()

    async def rest_delete(
        self,
        table: str,
        access_token: str,
        query_params: dict[str, str],
    ) -> None:
        url = f"{self._base}/rest/v1/{table}"
        r = await self._http.delete(
            url,
            headers=self._headers(access_token),
            params=query_params,
        )
        r.raise_for_status()

    async def webhook_post(self, url: str, payload: dict[str, Any], timeout: int = 60) -> None:
        r = await self._http.post(url, json=payload, timeout=timeout)
        r.raise_for_status()


supabase = SupabaseClient()

