"""Kreuzakt's HTTP API, narrowed to the calls this bridge may actually make.

Every shape here comes from a captured response, not from the API's appearance:
`docs/superpowers/probes/2026-11-17-kreuzakt-contracts/` (`capture-queue.json` for
upload/queue/retry/delete, `capture-search.json` for search/documents). Two
consequences of that evidence govern this whole module.

**There is no pagination on `/api/queue`.** `limit` (default 20, clamped to 100) and
`status` are the only accepted parameters; `page`, `offset`, `perPage`, `count`,
`after` and `cursor` are silently ignored, and the endpoint serves only the newest
100 rows. A page walk therefore re-reads one page forever, so a high-water mark
produced by walking is either wrong or a claim of exhaustion that cannot be made.
`queue_high_water_mark` is one `?limit=1` request instead: ordering is
`created_at DESC, id DESC` while ids ascend with insertion, so the first row of a
one-row request is the newest row.

**Absence from that window is not absence from the queue.** The window can saturate
(a `reindex-all`, another writer, our own retries), so `wait_for_queue_row` has three
outcomes — a matched row, `AmbiguousCorrelation`, and `CorrelationWindowFull`. The
last two are retryable (Task 6 re-uploads; identical bytes deduplicate onto one
document) and none of them is `None`, because a job that reads `None` as "nothing to
do" reports success while having indexed nothing.

`/api/search` and `/api/documents` are the opposite case: both *do* honour `page` and
`limit` (measured 2026-11-17: `page=2&limit=1` returned the second row, `limit=1000`
came back with `limit: 100`), so those two are exposed, and `offset` is not, because
it demonstrably did nothing on either endpoint.

**The rule that governs the parsing below.** This module is the only code that speaks
Kreuzakt HTTP, so a server surprise has to stop here rather than travel onward as data.
Every response is decoded through one guard, every body is required to be the shape its
type claims, and the fields the bridge writes back into Papra (`title`, `content`) or
matches queue rows on (`id`, `filename`, `status`, `relativePath`) are required to be
present rather than defaulted: `.get(key) or ""` turns "the server stopped sending this"
into a plausible empty value, and the next write then blanks a user's document with
nothing recorded anywhere. An unrecognised value is a `KreuzaktError` naming the keys the
server did send — never `""`, never `None`, never a `KeyError` from inside a poll loop.
"""

from __future__ import annotations

import asyncio
import posixpath
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx

# `/api/queue` clamps `limit` to 100 and serves only the newest 100 rows; that is both
# the biggest window there is and the whole window, which is why it is a constant
# rather than a parameter a caller can raise.
_QUEUE_WINDOW = 100
_SEARCH_LIMIT_MAX = 100
_LIMIT_MAX = 100
_TERMINAL_STATUSES = ("completed", "failed")

# The closed set of statuses `GET /api/queue` was measured to produce (2026-11-17:
# `pending`, `processing`, `completed`, `failed`, across every row of
# `capture-queue.json` and every `status=` filter). A status outside it is a server
# behaviour we have no evidence about, and the failure mode of guessing is specific and
# bad: an unrecognised status is not terminal, so a row carrying one would be polled for
# `index_wait_timeout_s` (900 s by default) and then reported as *absence* — which is the
# one answer this design forbids. Better to stop on the first read.
_QUEUE_STATUSES = ("pending", "processing", "completed", "failed")

# Kreuzakt-side paths and links. `KzDocument`/`KzHit` exist so that none of these reach a
# caller (the plan forbids `/api/documents/…`, `/original` or `/thumbnail` leaving the
# proxy as an identifier), so the one method that returns server dicts has to remove them
# itself rather than be the exception.
_LINK_KEYS = ("thumbnail_url", "download_url", "link_for_user", "stored_filename")


class KreuzaktError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class CorrelationWindowFull(KreuzaktError):
    """Every row the queue returned was newer than our mark and none of them was ours.

    Our row may well exist one row further down the table, where this API cannot show it:
    the window's own lower edge is above the mark, so nothing below that edge is provable.
    Judged from the rows actually returned rather than from `_QUEUE_WINDOW`, so a server
    cap that drops below 100 makes this fire earlier, never later — the old
    `len(newer) >= 100` would have downgraded a 5-row saturated window to "no queue row".
    Retryable by re-uploading; never report it as "no such row".
    """


class AmbiguousCorrelation(KreuzaktError):
    """More than one terminal row carries our filename above our mark.

    Measured: one upload of bytes that were already present produced a second row, and
    a re-upload of the same *name* produced a third, all with the same `document_id`.
    Picking one would attribute a document to the wrong attempt, so this is reported
    and retried by re-uploading instead.
    """


@dataclass(frozen=True)
class QueueEntry:
    """One row of `/api/queue`.

    `page_count` is joined from the document upstream, so it is null whenever there is
    no live document — it is carried for reporting, never used as a completion signal.
    """

    id: int
    filename: str
    status: str
    error: str | None
    document_id: int | None
    page_count: int | None


@dataclass(frozen=True)
class KzDocument:
    """The fields `GET /api/documents/{id}` returns that this bridge may read or write.

    Deliberately absent: `stored_filename`, `thumbnail_url`, `download_url`,
    `link_for_user`. Kreuzakt has no tenancy, so a path into its storage is not
    something a caller of this client should ever be handed.

    `title` and `content` are the two fields the bridge writes back into Papra, so they
    are required rather than defaulted: a body that stopped carrying them must not be
    turned into `""` and written over the user's text. `description`, `document_date` and
    `updated_at` stay optional because the bridge reads them but never overwrites Papra
    with an empty value on their account.

    `page_count` is nullable for the same reason it is nullable on `QueueEntry`: the
    measured document body carries it, and a null is the server's own absence rather than a
    shape surprise. It is here because the MCP document view reports it — it is the one fact
    about the file that Papra does not hold and the index does — and a `0` for a document whose
    count is unknown would be a claim about the PDF rather than a gap in knowledge. Still no path:
    `stored_filename`, `thumbnail_url`, `download_url` and `link_for_user` stay where they are.
    """

    id: int
    title: str
    description: str
    content: str
    document_date: str | None
    original_filename: str
    updated_at: str | None
    page_count: int | None = None


@dataclass(frozen=True)
class KzHit:
    """One row of `/api/search`. No `content` (the caller fetches the document when it
    needs the text, so a page of hits cannot blow up the response) and no `score`,
    which the measured response never carried.

    Every field declared `str` here is required on the wire: `or ""` did not make a
    missing value legal, it invented one the server never sent, and Task 13 orders its
    result list by `added_at` — where `""` reads as 'older than everything'.
    `document_date` is the only nullable one, and the capture shows it null.
    """

    id: int
    title: str
    document_date: str | None
    added_at: str
    mime_type: str
    original_filename: str


def _body_keys(body: Mapping[str, Any]) -> str:
    """The keys the server actually sent, for an error someone can act on."""
    return ", ".join(sorted(str(key) for key in body)) or "(no keys)"


def _require_json_object(body: Any, where: str) -> dict:
    """A JSON object, or a `KreuzaktError` a caller can classify.

    An HTML proxy page or a bare array answering 200 is a server surprise, not data;
    letting it through as `AttributeError: 'list' object has no attribute 'get'` makes it
    look like a bug in the caller.
    """
    if not isinstance(body, dict):
        raise KreuzaktError(
            f"unexpected {where}: {type(body).__name__}, expected a JSON object "
            f"(body starts: {str(body)[:120]!r})"
        )
    return body


def _require_str(body: Mapping[str, Any], key: str, where: str, why: str) -> str:
    """A present, non-null string — absent, null and wrong-typed are one failure."""
    value = body.get(key)
    if not isinstance(value, str):
        raise KreuzaktError(
            f"unexpected {where}: missing {key} (absent, null, or "
            f"{type(value).__name__}) — {why} "
            f"(body keys: {_body_keys(body)})"
        )
    return value


def _require_int(body: Mapping[str, Any], key: str, where: str, why: str, *,
                 positive: bool = False) -> int:
    """A present integer — and, for `positive`, one that can name a row.

    `positive` exists because a Kreuzakt id is not any integer, it is a SQLite rowid, and
    the code downstream of this client says so: `store.ledger_record_delivery_failure`
    refuses a non-positive `expected_kreuzakt_id` with `ValueError`, on the reasoning that
    `kreuzakt_id = NULL` matches no row so a refusal would silently charge nothing. That
    refusal is correct *and* it is not a `KreuzaktError`, so when a malformed body let `0`
    through here the `ValueError` arrived mid-pass in `verify_once` — which catches
    `PapraError`, `KreuzaktError` and `IndexFailed` and cannot be asked to catch a
    programming error — and the pass died for every reference after the one bad document.
    One malformed row is a corpus-wide blind spot, which is exactly the failure
    `verify_once`'s docstring promises it will not commit. So the malformed value is
    refused at the boundary that read it, as the typed error every caller already handles.
    """
    value = body.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise KreuzaktError(
            f"unexpected {where}: missing {key} — {value!r} is not an integer; {why} "
            f"(body keys: {_body_keys(body)})"
        )
    if positive and value <= 0:
        raise KreuzaktError(
            f"unexpected {where}: {key} is {value!r}, which cannot name a row — ids here "
            f"are counted from 1, and the ledger refuses to bind or charge a non-positive "
            f"one; {why} (body keys: {_body_keys(body)})"
        )
    return value


def _require_int_or_none(body: Mapping[str, Any], key: str, where: str) -> int | None:
    value = body.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise KreuzaktError(
            f"unexpected {where}: {key} is {value!r}, expected an integer or null "
            f"(body keys: {_body_keys(body)})"
        )
    return value


def _to_queue_entry(row: Any) -> QueueEntry:
    """One queue row, with the types `QueueEntry` claims.

    The keys are required and the status is validated here rather than in one call site,
    so `queue_page`, `queue_high_water_mark` and correlation all meet a malformed row the
    same way: as a `KreuzaktError`, not a bare `KeyError`/`TypeError` from inside a
    polling loop.
    """
    where = "/api/queue row"
    body = _require_json_object(row, where)
    entry_id = _require_int(body, "id", where,
                            "the id is the high-water mark and the correlation comparison",
                            positive=True)
    filename = _require_str(body, "filename", where,
                            "correlation matches queue rows on this value")
    status = _require_str(body, "status", where,
                          "terminal and non-terminal are read from it")
    if status not in _QUEUE_STATUSES:
        raise KreuzaktError(
            f"unknown queue status {status!r} on row {entry_id}: the measured statuses are "
            f"{', '.join(_QUEUE_STATUSES)} and anything else is a server behaviour this "
            "client would only be guessing about — treating an unrecognised status as "
            "non-terminal would poll it to the timeout and then report our upload as "
            "absent, which is the one conclusion that must never be drawn"
        )
    return QueueEntry(
        id=entry_id,
        filename=filename,
        status=status,
        error=body.get("error"),
        document_id=_require_int_or_none(body, "document_id", where),
        page_count=_require_int_or_none(body, "page_count", where),
    )


def _decode_json(response: httpx.Response, path: str) -> Any:
    """`response.json()` with nothing leaking past it.

    A 200 that is an HTML proxy page raises `json.JSONDecodeError` (a `ValueError`) from
    `httpx`, and a 200 whose body is an array raises `AttributeError` one line later.
    Neither is a `KreuzaktError`, so a caller that catches `KreuzaktError` — which is all
    Task 6 does — would die on a message it cannot classify as a Kreuzakt failure.
    """
    try:
        return response.json()
    except ValueError as error:
        raise KreuzaktError(
            f"{path} returned a body that is not JSON "
            f"(starts: {response.text[:120]!r}, content-type: "
            f"{response.headers.get('content-type')!r})"
        ) from error


def _window_summary(rows: list[QueueEntry], after_id: int) -> str:
    """What the last window actually held, for a timeout message a person will read.

    Deliberately does not call `max(ids)` "the newest id above the mark": for a window
    that reaches below the mark — the usual case once the queue is quiet — the newest row
    in it can be *below* the mark, and that phrasing sent readers to a table with no rows.
    """
    if not rows:
        return "the queue window came back empty, no row of any name"
    ids = [row.id for row in rows]
    newer_count = sum(1 for entry_id in ids if entry_id > after_id)
    return (f"the window held {len(rows)} rows (ids {max(ids)}..{min(ids)}), "
            f"{newer_count} of them newer than id {after_id}")


def _is_our_row(stored: str, uploaded: str) -> bool:
    """Exact equality, or the ` (N)` collision suffix. Nothing else.

    Measured: `filename` is the stored name, byte-identical to the upload response's
    `relativePath`, and a repeat of a name comes back as `papra-doc0006-6f708192
    (1).pdf`. Kreuzakt also *sanitises* unsafe characters (`papra:doc0005<…>.pdf` →
    `papra_doc0005_…_.pdf`), and that is not reimplemented here: Task 6 constrains the
    upload name to `[a-z0-9.-]` plus `.pdf`, so a sanitised name can only mean the
    server changed something it should not have, and matching it would be a guess.

    Suffix matching in particular would be wrong: `reindex-all` rows end with the
    original filename (`reindex-all:<batch>:15:papra-seq0004-9e9e9e9e.pdf`), so any
    suffix rule would read a re-extraction as one of our uploads.
    """
    if stored == uploaded:
        return True
    stem, extension = posixpath.splitext(uploaded)
    prefix = f"{stem} ("
    suffix = f"){extension}"
    if not (stored.startswith(prefix) and stored.endswith(suffix)):
        return False
    counter = stored[len(prefix):len(stored) - len(suffix)]
    # ASCII digits only. `str.isdigit()` is also True for `\u0663` (Arabic-Indic three) and
    # `\u00b2`, so in an index another writer shares with us `papra-d1-a1b2 (\u0663).pdf` would
    # have been matched as our `papra-d1-a1b2.pdf` and its `document_id` attributed to our
    # job — a wrong document indexed under the wrong id, silently. Kreuzakt's own counter
    # is an ASCII integer, so anything else cannot be its collision suffix.
    return counter.isascii() and counter.isdigit()


class KreuzaktClient:
    def __init__(self, base_url: str, *, timeout_s: float = 30.0,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_s,
            transport=transport,
        )

    async def __aenter__(self) -> KreuzaktClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            response = await self._client.request(method, path, **kwargs)
        except httpx.HTTPError as error:
            # No status: there was no response. Retry policy is the caller's (Task 6
            # bounds it with `index_max_attempts`), because a deterministic failure
            # here re-fails identically and an unbounded retry would spin on it.
            raise KreuzaktError(
                f"{method} {path} failed: {type(error).__name__}: {error}"
            ) from error
        if response.is_success:
            return response
        raise KreuzaktError(
            f"{method} {path} failed: {response.status_code} {response.text[:200]}",
            response.status_code,
        )

    async def _get_json(self, path: str, **params: Any) -> Any:
        response = await self._request("GET", path, params=params or None)
        return _decode_json(response, path)

    async def health(self) -> dict:
        """`GET /api/health` as the object it is, and only if it says whether it is ok.

        The plan's rebuild safety (Task 11, `ledger --invalidate`) refuses to run unless
        `health()["status"] == "ok"`, so this returns a JSON object that carries `status`.
        A body that is a list, a string, or an object without `status` used to be handed
        back verbatim and reach that check as a `TypeError`/`KeyError` — indistinguishable,
        in a log, from a dead index.
        """
        body = _require_json_object(await self._get_json("/api/health"), "/api/health body")
        _require_str(body, "status", "/api/health body",
                     "readiness is decided by this field, so a body without it cannot be "
                     "reported as healthy or unhealthy")
        return body

    async def queue_counts(self) -> dict[str, int]:
        """Queue depth by status. `counts` is a global aggregate of the envelope, so
        the one-row request that also carries it costs nothing extra.

        Every value must already be an integer: `int(value)` raised a bare `ValueError` on
        `"many"` and silently accepted `"3"`, and a count this client had to guess at is a
        changed envelope, not a zero.
        """
        body = await self._get_json("/api/queue", limit=1)
        counts = body.get("counts") if isinstance(body, dict) else None
        if not isinstance(counts, dict):
            raise KreuzaktError("unexpected /api/queue envelope: no counts block")
        result: dict[str, int] = {}
        for key, value in counts.items():
            if isinstance(value, bool) or not isinstance(value, int):
                raise KreuzaktError(
                    f"unexpected /api/queue counts.{key}: {value!r} is not an integer count"
                )
            result[str(key)] = value
        return result

    async def queue_page(self, *, limit: int = _QUEUE_WINDOW,
                         status: str | None = None) -> list[QueueEntry]:
        """The newest `limit` rows (server caps at 100), optionally one status.

        No `page`/`offset`: they are ignored upstream, so exposing them here would let
        a caller believe it had asked for something else.
        """
        params: dict[str, Any] = {"limit": max(1, min(limit, _QUEUE_WINDOW))}
        if status is not None:
            params["status"] = status
        body = await self._get_json("/api/queue", **params)
        rows = body.get("entries") if isinstance(body, dict) else None
        if not isinstance(rows, list):
            raise KreuzaktError(
                "unexpected /api/queue envelope: no entries list "
                f"(body keys: {sorted(body) if isinstance(body, dict) else type(body).__name__})"
            )
        return [_to_queue_entry(row) for row in rows]

    async def queue_high_water_mark(self) -> int:
        """Newest queue id right now, or 0 for an empty queue. Recorded BEFORE upload.

        Measured 2026-11-17: `/api/queue` has no pagination (only `limit`, clamped to
        100, and `status`; `page`/`offset`/`cursor` are ignored), it serves the newest
        100 rows, and it orders them `created_at DESC, id DESC` while ids ascend with
        insertion. So the newest row is the first row of a one-row request: one round
        trip, and no walk to get wrong.

        What this can no longer claim is *exhaustion* — a newest-100 window cannot be
        proven complete — which is why `wait_for_queue_row` reports a saturated window
        as `CorrelationWindowFull` rather than as "never uploaded".

        **Clock caveat, stated because it is the only thing that breaks this.** Reading
        `entries[0].id` as the newest id is sound only while `created_at DESC, id DESC`
        coincides with `id DESC`, i.e. while the Kreuzakt host clock never runs backwards.
        If it jumps back (NTP correction, a VM resumed with a stale clock, a restored
        volume), a row inserted later can sort below a row inserted earlier, and then
        `entries[0].id` is *lower* than the true newest id. The mark is then too low, and
        a row that already existed before our upload — another writer's, or one of our own
        earlier attempts — sits above it and can be correlated as if it were ours. Nothing
        here can detect that from a newest-100 window; a host whose clock moves is visible
        only as a document indexed under the wrong attempt, so this is an operator
        assumption, not a check. `id` itself is monotonic (measured: ids ascend with
        insertion), which is why the mark is an id and not a timestamp.
        """
        rows = await self.queue_page(limit=1)
        return rows[0].id if rows else 0

    async def wait_for_queue_row(self, filename: str, *, after_id: int, poll_s: float,
                                 timeout_s: float) -> QueueEntry:
        """The one queue row our upload produced, or a typed failure.

        One match, three failures:

        * a `QueueEntry` in `completed` or `failed` — the matched row;
        * `AmbiguousCorrelation` — more than one terminal row matches, so no row can be
          picked honestly;
        * `CorrelationWindowFull` — every row the request returned was newer than
          `after_id` and none of them was ours, which says nothing about whether our row
          exists (the rule is read off the returned window, not off `_QUEUE_WINDOW`, so it
          still holds if the server's cap ever drops);
        * `KreuzaktError("no terminal queue row …")` once `timeout_s` has passed while our
          row sat in the window unfinished — the row exists, the answer does not yet;
        * `KreuzaktError("no queue row …")` once `timeout_s` has passed with the window
          readable, our row not in it, and its lower edge at or below the mark — the only
          case where absence is actually observable.

        `filename` must be the `relativePath` the upload response returned, not the name
        we constructed. Matching is exact plus the ` (N)` collision suffix, and the id
        comparison is strictly greater than the mark recorded before the upload: rows at
        or below it are earlier attempts at the same name.
        """
        deadline = time.monotonic() + timeout_s
        while True:
            rows = await self.queue_page(limit=_QUEUE_WINDOW)
            candidates = [row for row in rows
                          if row.id > after_id and _is_our_row(row.filename, filename)]
            terminal = [row for row in candidates if row.status in _TERMINAL_STATUSES]
            if len(terminal) > 1:
                raise AmbiguousCorrelation(
                    f"ambiguous correlation: {len(terminal)} terminal queue rows above id "
                    f"{after_id} for {filename} (ids {[row.id for row in terminal]})"
                )
            if len(terminal) == 1:
                row = terminal[0]
                if row.status == "completed" and row.document_id is None:
                    # Measured: deleting a document nulls `document_id` on every row
                    # that pointed at it while the row stays `completed`. So this is a
                    # vanished document, and re-running the queue row is not even
                    # possible (`retry` accepts only `failed`). The caller decides by
                    # reading Papra's own state, so hand it the error, not a row.
                    raise KreuzaktError(
                        f"queue row {row.id} for {filename} is completed without document_id"
                    )
                return row
            newer = [row for row in rows if row.id > after_id]
            # Saturation is read off the window, not off `_QUEUE_WINDOW`: if every row the
            # request returned is newer than the mark, the window's own lower edge is
            # above the mark, so rows between the mark and that edge exist in a region
            # this endpoint cannot show us — whatever the server's current cap happens to
            # be. `len(newer) >= 100` was a claim about our constant, and if the cap ever
            # dropped it would silently downgrade saturation to "no queue row".
            if not candidates and newer and len(newer) == len(rows):
                raise CorrelationWindowFull(
                    f"correlation window full: all {len(rows)} rows this request returned "
                    f"are newer than id {after_id} and none is named {filename}; the oldest "
                    f"row this API will show is id {min(row.id for row in rows)}, so rows "
                    "between the mark and there are invisible and this is not evidence of "
                    "absence"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if candidates:
                    detail = ", ".join(f"id {row.id} is {row.status}"
                                       for row in candidates)
                    raise KreuzaktError(
                        f"no terminal queue row for {filename} within {timeout_s}s: "
                        f"our row is in the window but has not finished — {detail}; "
                        f"{_window_summary(rows, after_id)}"
                    )
                raise KreuzaktError(
                    f"no queue row for {filename} within {timeout_s}s: "
                    f"{_window_summary(rows, after_id)}"
                )
            await asyncio.sleep(min(poll_s, remaining) if poll_s > 0 else min(0.01, remaining))

    async def upload(self, filename: str, content: bytes) -> str:
        """Upload one file and return the name Kreuzakt stored it under.

        The multipart field is `files` (measured: a wrong field name is a 400 whose
        verbatim message is `Upload requests must include at least one file`), and one
        request carries exactly one file: the correlation window is 100 rows, so a
        caller that fires several uploads before correlating is gambling that nothing
        else wrote 90 rows in between.

        The return value is the server's `relativePath`, not `filename`: sanitising and
        the collision suffix both rename the row under us, and the stored name is the
        only value guaranteed to be what the queue will report.
        """
        response = await self._request(
            "POST", "/api/upload",
            files={"files": (filename, content, "application/pdf")},
        )
        payload = _require_json_object(_decode_json(response, "/api/upload"),
                                       "/api/upload body")
        if payload.get("failedCount") or not payload.get("files"):
            raise KreuzaktError(f"upload rejected: {payload.get('errors')!r}")
        first = payload["files"][0] if isinstance(payload["files"], list) else None
        if not isinstance(first, dict):
            raise KreuzaktError(
                f"upload response has no usable relativePath: files[0] is "
                f"{type(first).__name__}, expected a JSON object with a `relativePath` "
                f"(files: {str(payload['files'])[:200]!r})"
            )
        relative_path = first.get("relativePath")
        if not isinstance(relative_path, str) or not relative_path:
            raise KreuzaktError(
                f"upload response has no usable relativePath: it is "
                f"{relative_path!r}, and it is the exact value correlation matches queue "
                f"rows against, so it cannot be guessed "
                f"(files[0] keys: {_body_keys(first)})"
            )
        return relative_path

    async def get_document(self, document_id: int) -> KzDocument:
        """One document, narrowed to `KzDocument`.

        `title` and `content` are required on the wire because the bridge writes both back
        into Papra: `body.get("content") or ""` turned a dropped or null key into an empty
        document, and the next write would blank the user's text with nothing recorded
        anywhere that the server had stopped answering in full. A value the server really
        did send as `""` is kept as `""` — that is the truth, not a gap.
        """
        where = f"/api/documents/{document_id} body"
        body = _require_json_object(await self._get_json(f"/api/documents/{document_id}"),
                                    where)
        entry_id = _require_int(body, "id", where,
                                "it is the id the ledger stores next to the Papra document",
                                positive=True)
        title = _require_str(body, "title", where,
                             "it is written back into Papra's title, so an absent one must "
                             "not become an empty one")
        content = _require_str(body, "content", where,
                               "it is written back into Papra's document text, so an absent "
                               "one must not blank it")
        return KzDocument(
            id=entry_id,
            title=title,
            description=body.get("description") or "",
            content=content,
            document_date=body.get("document_date"),
            original_filename=body.get("original_filename") or "",
            updated_at=body.get("updated_at"),
            page_count=_require_int_or_none(body, "page_count", where),
        )

    async def delete_document(self, document_id: int) -> None:
        """Delete a document. 404 counts as done: already gone is gone.

        Measured side effect worth knowing when the caller reads a queue row: the delete
        nulls `document_id` on every queue row that referenced this document and leaves
        the rows themselves in place, `completed`.
        """
        try:
            await self._request("DELETE", f"/api/documents/{document_id}")
        except KreuzaktError as error:
            if error.status == 404:
                return None
            raise
        return None

    async def search(self, query: str, *, page: int = 1,
                     limit: int = _SEARCH_LIMIT_MAX) -> tuple[list[KzHit], int]:
        """One page of hits plus the server's `total`.

        Measured 2026-11-17: `q`, `page` (1-based) and `limit` (default 20, clamped to
        100) all do something — `page=2&limit=1` returned the second row and `limit=1000`
        came back with `limit: 100`. `offset`, `pageIndex` and `pageSize` did nothing at
        all, so they are not parameters here. A page beyond the end is a 200 with no
        results, not an error, and `total` still reports the full match count.

        A hit carries `content`, `snippet` and `thumbnail_url`; none of them reach the
        caller — see `KzHit`.
        """
        params = {
            "q": query,
            "page": max(1, page),
            "limit": max(1, min(limit, _SEARCH_LIMIT_MAX)),
        }
        body = await self._get_json("/api/search", **params)
        rows = body.get("results") if isinstance(body, dict) else None
        total = body.get("total") if isinstance(body, dict) else None
        if not isinstance(rows, list) or not isinstance(total, int):
            raise KreuzaktError(
                "unexpected /api/search envelope: need `results` and `total` "
                f"(body keys: {sorted(body) if isinstance(body, dict) else type(body).__name__})"
            )
        hits = [_hit_from_row(row) for row in rows]
        return hits, total

    async def list_documents(self, *, page: int = 1, limit: int = _LIMIT_MAX,
                             since: str | None = None) -> list[dict]:
        """Rows of `GET /api/documents` in the server's own order (newest first).

        Measured: the envelope is `{documents, total, page, limit}` and a row carries
        `id, title, description, document_date, added_at, original_filename,
        mime_type, thumbnail_url` — no `content`. `limit` is honoured and clamped to
        100; `page` is honoured; `offset` is ignored and so is not offered.

        **Which keys are removed, and why.** `thumbnail_url`, `download_url`,
        `link_for_user` and `stored_filename` are stripped from every row
        (`_LINK_KEYS`). This is the one method that hands out the server's own dicts
        instead of a narrowed type, so it has to do by hand the narrowing `KzDocument` and
        `KzHit` do by construction: the plan's rule is that no Kreuzakt-side path
        (`/api/documents/…`, `/thumbnail`, `/original`) or Kreuzakt storage name may leave
        the proxy as an identifier, and a measured row carries
        `"thumbnail_url": "/api/documents/4/thumbnail"`. `stored_filename` is stripped too
        although the list endpoint did not send it, so that a future row cannot smuggle it
        out through here. Every other key is passed through unchanged.

        `since` is honoured but has one trap: Kreuzakt compares it against a
        `YYYY-MM-DD HH:MM:SS` column, so `"2026-11-17 22:28:05"` filters while the same
        instant as `"2026-11-17T22:28:05Z"` matched *nothing* (200, `total: 0`) — a
        silent empty result, which for an incremental scan means "nothing changed,
        forever". A value Kreuzakt would silently ignore is refused here instead; which
        column it filters was not determined, so `since` is a coarse gate and the
        caller must not read it as a watermark.
        """
        params: dict[str, Any] = {"page": max(1, page),
                                  "limit": max(1, min(limit, _LIMIT_MAX))}
        if since is not None:
            params["since"] = _require_documents_since(since)
        body = await self._get_json("/api/documents", **params)
        rows = body.get("documents") if isinstance(body, dict) else None
        if not isinstance(rows, list):
            raise KreuzaktError(
                "unexpected /api/documents envelope: no documents list "
                f"(body keys: {sorted(body) if isinstance(body, dict) else type(body).__name__})"
            )
        cleaned: list[dict] = []
        for row in rows:
            body = _require_json_object(row, "/api/documents row")
            cleaned.append({key: value for key, value in body.items()
                            if key not in _LINK_KEYS})
        return cleaned

    async def retry_queue(self, entry_id: int) -> None:
        """Re-queue a `failed` row.

        The upstream 400/404 distinction survives verbatim, because the two mean
        different things to the caller: 400 `Queue entry 31 is not in failed status`
        means the row already finished (so the answer is to read the row, not retry it),
        404 `Queue entry 999999 not found` means the row is gone.

        Measured: a retry **reuses** the row rather than appending one, and a
        deterministic failure re-fails with identical text, so the attempt counter that
        bounds this lives in the job, not here.
        """
        await self._request("POST", f"/api/queue/{entry_id}/retry")


_DOCUMENTS_SINCE = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$")


def _hit_from_row(row: Any) -> KzHit:
    """One search hit, with the fields `KzHit` declares actually present.

    A missing or null `added_at` used to become `""`, which the declared `str` cannot
    express and which sorts as the oldest document in Task 13's result list — a wrong
    order presented as a ranking.
    """
    where = "/api/search row"
    body = _require_json_object(row, where)
    return KzHit(
        id=_require_int(body, "id", where, "it is the Kreuzakt id the ledger resolves",
                        positive=True),
        title=_require_str(body, "title", where, "it is shown to the user as the hit title"),
        document_date=body.get("document_date"),
        added_at=_require_str(body, "added_at", where,
                              "results are ordered by it, so a gap would reorder them"),
        mime_type=_require_str(body, "mime_type", where,
                               "it decides whether the caller may open the file as a PDF"),
        original_filename=_require_str(body, "original_filename", where,
                                       "it is shown as the file name"),
    )


def _require_documents_since(since: str) -> str:
    """Accept only the two timestamp shapes `/api/documents` demonstrably filters on.

    Measured: `"2026-11-17 22:28:05"` and `"2026-11-17"` filter, while
    `"2026-11-17T22:28:05Z"` — the same instant — came back 200 with **no** rows, and
    `"1700000000"` came back with **all** of them. Both wrong answers look like a
    working request: one means an incremental scan concludes nothing ever changed, the
    other means it re-reads everything. The comparison upstream is a lexical string
    comparison against a `YYYY-MM-DD HH:MM:SS` column, so only that shape (or its date
    prefix) is a filter at all.
    """
    if not _DOCUMENTS_SINCE.match(since):
        raise KreuzaktError(
            f"since={since!r} is not a Kreuzakt document watermark: `since` must be "
            "'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' (measured: an ISO-8601 value with "
            "'T'/'Z' returns 200 with no rows, and an epoch integer returns every row, "
            "so neither can be sent as if it filtered)"
        )
    return since
