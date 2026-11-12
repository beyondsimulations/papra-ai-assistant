import asyncio

import httpx

MAX_ATTEMPTS = 3



class PapraError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class PapraClient:
    def __init__(self, base_url: str, api_key: str, *, timeout_s: float = 30.0,
                 backoff_base_s: float = 0.5,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_s,
            transport=transport,
        )
        self._backoff_base_s = backoff_base_s

    async def __aenter__(self) -> "PapraClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        last_error = ""
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = await self._client.request(method, path, **kwargs)
            except httpx.TransportError as error:
                last_error = str(error)
                if attempt == MAX_ATTEMPTS - 1:
                    break
                await asyncio.sleep(self._backoff_base_s * 4**attempt)
                continue
            if (response.status_code == 429 or response.status_code >= 500) \
                    and attempt < MAX_ATTEMPTS - 1:
                await asyncio.sleep(self._backoff_base_s * 4**attempt)
                continue
            if response.is_success:
                return response
            raise PapraError(
                f"{method} {path} failed: {response.status_code} {response.text[:200]}",
                response.status_code,
            )
        raise PapraError(f"{method} {path} failed after {MAX_ATTEMPTS} attempts: {last_error}")

    async def create_document(self, org_id: str, filename: str, content: bytes) -> dict:
        response = await self._request(
            "POST", f"/api/organizations/{org_id}/documents",
            files={"file": (filename, content, "application/pdf")},
        )
        return response.json()["document"]

    async def get_document(self, org_id: str, document_id: str) -> dict:
        response = await self._request(
            "GET", f"/api/organizations/{org_id}/documents/{document_id}"
        )
        return response.json()["document"]

    async def update_document(self, org_id: str, document_id: str, *,
                              name: str | None = None, content: str | None = None) -> dict:
        body = {key: value for key, value in {"name": name, "content": content}.items()
                if value is not None}
        response = await self._request(
            "PATCH", f"/api/organizations/{org_id}/documents/{document_id}", json=body
        )
        return response.json()["document"]

    async def list_documents(self, org_id: str, *, page_index: int = 0, page_size: int = 100,
                             search_query: str | None = None, sort_field: str = "createdAt",
                             sort_order: str = "desc") -> tuple[list[dict], int]:
        params = {
            "pageIndex": page_index, "pageSize": page_size,
            "sortField": sort_field, "sortOrder": sort_order,
        }
        if search_query is not None:
            params["searchQuery"] = search_query
        response = await self._request(
            "GET", f"/api/organizations/{org_id}/documents", params=params
        )
        data = response.json()
        return data["documents"], data.get("documentsCount", len(data["documents"]))

    async def download_file(self, org_id: str, document_id: str) -> bytes:
        response = await self._request(
            "GET", f"/api/organizations/{org_id}/documents/{document_id}/file"
        )
        return response.content

    async def list_tags(self, org_id: str) -> list[dict]:
        response = await self._request("GET", f"/api/organizations/{org_id}/tags")
        return response.json()["tags"]

    async def add_tag(self, org_id: str, document_id: str, tag_id: str) -> None:
        await self._request(
            "POST", f"/api/organizations/{org_id}/documents/{document_id}/tags",
            json={"tagId": tag_id},
        )

    async def whoami(self) -> dict:
        response = await self._request("GET", "/api/api-keys/current")
        return response.json()["apiKey"]
