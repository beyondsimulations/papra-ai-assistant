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
            if (
                response.status_code == 429 or response.status_code >= 500
            ) and attempt < MAX_ATTEMPTS - 1:
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

    #: The client's keyword to Papra's field name. One place, because the two disagree on
    #: exactly one column and a caller that guessed would send a key Papra ignores — which
    #: answers 200 and changes nothing, the worst possible failure for a delivery flag.
    _WIRE_FIELD = {"name": "name", "content": "content", "document_date": "documentDate",
                   "notes": "notes"}

    async def update_document(self, org_id: str, document_id: str, *,
                              name: str | None = None, content: str | None = None,
                              document_date: str | None = None,
                              notes: str | None = None) -> dict:
        """Patch the fields this call names, and only those.

        Three facts about the body come from the pinned container rather than from taste,
        and all three are the kind that answer 200 for the request and lose the field:
        `documentDate` is the column, not `document_date`; a bare ISO date is accepted and
        stored as `2025-10-01T00:00:00.000Z`; and `notes` stops accepting at 2048
        characters, above which Papra answers 400 and the whole PATCH — title included —
        fails. An empty `name` is a 400 too, so nothing here should ever pass one.

        Omitted means untouched, which is what makes a partial patch safe: a delivery that
        sends `{"notes": ...}` cannot rename a document a human titled. `None` is filtered
        rather than sent, because Papra has no "clear this field" in this endpoint and an
        explicit null would be a guess at one.
        """
        supplied = {"name": name, "content": content, "document_date": document_date,
                    "notes": notes}
        body = {self._WIRE_FIELD[key]: value
                for key, value in supplied.items() if value is not None}
        response = await self._request(
            "PATCH", f"/api/organizations/{org_id}/documents/{document_id}", json=body
        )
        return response.json()["document"]

    async def update_content(self, org_id: str, document_id: str, content: str) -> None:
        """Write the text column and nothing else.

        `update_document` is the general write and takes whatever it is handed, which is
        exactly the hazard once this bridge owns Papra's text: one extra argument in a
        delivery and a content update renames a document a human titled, or clears a note
        nobody meant to touch. Naming the field in the method name is the narrow version
        of the capability, and it is what Task 7's content writer is allowed to use.

        Returns nothing on purpose. The response body is Papra's document, and a caller
        that receives it will be tempted to read `content` back from it; the ledger records
        what we sent, and the next verification pass reads Papra's own answer.
        """
        await self.update_document(org_id, document_id, content=content)

    async def list_documents(self, org_id: str, *, page_index: int = 0, page_size: int = 100,
                             search_query: str | None = None, sort_field: str = "createdAt",
                             sort_order: str = "desc") -> tuple[list[dict], int | None]:
        """One page, and the total the server declared beside it (`None` if it declared none).

        The total is returned as it was answered rather than as `len(documents)` when
        `documentsCount` is missing, because the one caller that reads it as a completeness
        claim cannot use a substituted number at all: `rows_seen < total` is the check that a
        listing walk finished, and a total fabricated from the page length makes it false by
        construction. A shape change upstream would then switch the guard off quietly, which
        is worse than a guard that refuses to run — so the absence travels to the caller, and
        the caller aborts the pass on it. Callers that want a count of rows have `len(...)`.
        """
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
        declared = data.get("documentsCount")
        return data["documents"], declared if isinstance(declared, int) else None

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
