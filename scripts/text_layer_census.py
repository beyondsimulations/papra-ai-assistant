"""How much of the archive would a degraded content handover actually touch?

Read-only. Never writes to Papra, never calls Kreuzakt, never spends an inference token.

**Why this exists.** Kreuzakt's TEXT-LAYER extraction path drops the top band of a page — measured
on the operator's own corpus at roughly the top 40-60 points, which is where letterhead, the
addressee block and the word "Rechnung" live. Its OCR path does not drop it, and Papra's own OCR
does not either. So letting the index's text overwrite Papra's text costs something only for a
document that (a) already carries an embedded text layer and (b) has text inside that band.
Everything else is either re-OCRed from the page image, where neither side got the band for free,
or digital with an empty top margin.

That set is the size of the risk behind "what should delivery do when the index's text is materially
shorter than Papra's?" — and it must be measured *before* anything is overwritten, because there is
no undo: Papra keeps no previous version of the `content` column.

Sampling is deterministic rather than random: listing order is stable, so every Nth document is
taken and the same corpus yields the same sample. That is what makes a number quoted in a runbook
reproducible a month later. The projection says exactly what it is — a Wilson score interval over
the sample, scaled by the bucket's own PDF count — and prints the sample size beside it so nobody
reads a 20-document sample as a census.

Usage, on atlas, with the suite's own environment:

    .venv/bin/python scripts/text_layer_census.py
    .venv/bin/python scripts/text_layer_census.py --org-id org_xxx --per-bucket 30 --top-band-pt 45

Buckets come from the suite's store (the configured ones plus anything the ledger has ever seen),
the same rule `backfill` uses, because Papra is never asked for a bucket list — the API key defines
what it may read. Pass `--org-id` to name one the store has forgotten.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import statistics
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import fitz  # PyMuPDF

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from papra_suite.config import ConfigError, load_config  # noqa: E402
from papra_suite.papra_client import PapraClient, PapraError  # noqa: E402
from papra_suite.store import Store  # noqa: E402

#: Band text counts only if it is at least this long: a stray glyph encoded by the generator is
#: not a letterhead.
MIN_BAND_CHARS = 12
#: A page is "digital" when its embedded layer carries at least this many characters overall.
MIN_PAGE_CHARS = 40
#: The strip was measured between 40 and 60 points, so the default is the wider bound: a document is
#: counted at risk even if the real strip turns out to be narrower.
DEFAULT_TOP_BAND_PT = 60.0
PAGE_SIZE = 100
#: A bucket with more than this many documents is not this archive; stop rather than spin.
MAX_PAGES_PER_BUCKET = 200


@dataclass(frozen=True)
class PageText:
    total_chars: int
    band_chars: int

    @property
    def digital(self) -> bool:
        return self.total_chars >= MIN_PAGE_CHARS

    @property
    def band_text(self) -> bool:
        return self.band_chars >= MIN_BAND_CHARS


@dataclass
class DocVerdict:
    document_id: str
    name: str
    pages: int = 0
    digital_pages: int = 0
    band_pages: int = 0
    total_chars: int = 0
    error: str | None = None

    @property
    def kind(self) -> str:
        """`scanned`, `digital-clean`, `digital-at-risk`, or `error`."""
        if self.error is not None:
            return "error"
        if self.pages == 0:
            return "error"
        if self.digital_pages == 0:
            return "scanned"
        return "digital-at-risk" if self.band_pages else "digital-clean"


@dataclass
class BucketResult:
    org_id: str
    listed: int
    pdfs: int
    sampled: int = 0
    verdicts: list[DocVerdict] = field(default_factory=list)

    def count(self, kind: str) -> int:
        return sum(1 for v in self.verdicts if v.kind == kind)

    def wilson(self, hits: int, n: int, z: float = 1.96) -> tuple[float, float]:
        """Wilson interval for the sample share, scaled to this bucket's own PDF count."""
        if n == 0:
            return (0.0, 0.0)
        p = hits / n
        denom = 1 + z * z / n
        centre = (p + z * z / (2 * n)) / denom
        half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
        return (max(0.0, centre - half) * self.pdfs, min(1.0, centre + half) * self.pdfs)


def classify_page(page: fitz.Page, top_band_pt: float) -> PageText:
    """Embedded text for the whole page and for its top band.

    `clip` measures from the top-left of the page box in points, the same geometry the strip was
    measured in, so no rotation or crop correction is applied on purpose: the census is meant to see
    what the extractor sees, including pages whose box is unusual.
    """
    total = page.get_text("text") or ""
    rect = page.rect
    band = fitz.Rect(rect.x0, rect.y0, rect.x1, min(rect.y1, rect.y0 + top_band_pt))
    band_text = page.get_text("text", clip=band) or ""
    return PageText(total_chars=len(total.strip()), band_chars=len(band_text.strip()))


def classify_document(document_id: str, name: str, payload: bytes,
                      top_band_pt: float) -> DocVerdict:
    verdict = DocVerdict(document_id=document_id, name=name)
    try:
        with fitz.open(stream=payload, filetype="pdf") as doc:
            for page in doc:
                text = classify_page(page, top_band_pt)
                verdict.pages += 1
                verdict.total_chars += text.total_chars
                if text.digital:
                    verdict.digital_pages += 1
                if text.band_text:
                    verdict.band_pages += 1
    except Exception as error:  # a PDF fitz will not open is a datum, not a crash
        verdict.error = f"{type(error).__name__}: {error}"
    return verdict


def is_pdf(doc: dict) -> bool:
    return (str(doc.get("name") or "").lower().endswith(".pdf")
            or str(doc.get("mimeType") or "") == "application/pdf")


def take_sample(items: list[dict], cap: int) -> list[dict]:
    """Every Nth PDF, deterministically, until `cap` are taken."""
    pdfs = [d for d in items if is_pdf(d)]
    if cap <= 0 or len(pdfs) <= cap:
        return pdfs
    step = len(pdfs) / cap
    return [pdfs[int(i * step)] for i in range(cap)]


async def list_all(client: PapraClient, org_id: str) -> tuple[list[dict], int | None]:
    """Every document in a bucket, plus the total Papra declared beside the last page."""
    rows: list[dict] = []
    declared: int | None = None
    page_index = 0
    while page_index < MAX_PAGES_PER_BUCKET:
        chunk, total = await client.list_documents(org_id, page_index=page_index,
                                                   page_size=PAGE_SIZE)
        if total is not None:
            declared = total
        rows.extend(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        page_index += 1
    return rows, declared


async def collect_bucket(client: PapraClient, org_id: str, cap: int,
                         top_band_pt: float) -> BucketResult:
    items, declared = await list_all(client, org_id)
    result = BucketResult(org_id=org_id, listed=(declared if declared is not None else len(items)),
                          pdfs=sum(1 for d in items if is_pdf(d)))
    sample = take_sample(items, cap)
    result.sampled = len(sample)
    for doc in sample:
        document_id = str(doc.get("id"))
        name = str(doc.get("name") or "")
        try:
            payload = await client.download_file(org_id, document_id)
        except PapraError as error:
            result.verdicts.append(DocVerdict(document_id, name, error=str(error)[:140]))
            continue
        result.verdicts.append(classify_document(document_id, name, payload, top_band_pt))
    return result


def known_orgs(store: Store) -> list[str]:
    known = {org.papra_org_id for org in store.list_orgs()}
    known.update(store.ledger_org_ids())
    return sorted(known)


def _bar(fraction: float, width: int = 18) -> str:
    filled = int(round(max(0.0, min(1.0, fraction)) * width))
    return "#" * filled + "." * (width - filled)


async def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Measure the top-band exposure of the archive")
    parser.add_argument("--org-id", action="append", dest="org_ids",
                        help="bucket to census; repeatable. Default: every bucket the store knows")
    parser.add_argument("--per-bucket", type=int, default=60,
                        help="PDFs to download per bucket (default 60)")
    parser.add_argument("--top-band-pt", type=float, default=DEFAULT_TOP_BAND_PT,
                        help=f"height of the dropped band in pt "
                             f"(default {DEFAULT_TOP_BAND_PT})")
    parser.add_argument("--show", type=int, default=8,
                        help="print at most this many names per bucket")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.per_bucket < 1 or args.top_band_pt <= 0:
        print("--per-bucket must be >= 1 and --top-band-pt must be > 0", file=sys.stderr)
        return 2

    try:
        config = load_config(dict(os.environ))
    except ConfigError as error:
        print(f"configuration refused: {error}", file=sys.stderr)
        return 2

    store = Store(Path(config.data_dir) / "state.db")
    org_ids = args.org_ids or known_orgs(store)
    if not org_ids:
        print("no buckets known: pass --org-id (the store has none, and Papra is never asked for a "
              "bucket list because the API key defines what it may read)", file=sys.stderr)
        return 2

    total_pdf = total_at_risk = total_sampled = 0
    pages_seen: list[int] = []
    async with PapraClient(base_url=config.papra_base_url, api_key=config.papra_api_key) as client:
        for org_id in org_ids:
            result = await collect_bucket(client, org_id, args.per_bucket, args.top_band_pt)
            n = len(result.verdicts)
            scanned, clean = result.count("scanned"), result.count("digital-clean")
            at_risk, broken = result.count("digital-at-risk"), result.count("error")
            total_pdf += result.pdfs
            total_at_risk += at_risk
            total_sampled += n
            pages_seen.extend(v.pages for v in result.verdicts if v.error is None)
            low, high = result.wilson(at_risk, n)
            print(f"\n{org_id}")
            print(f"  {result.listed} documents listed, {result.pdfs} PDFs; sampled {n}")
            if n:
                print(f"  scanned         {_bar(scanned / n)} {scanned}")
                print(f"  digital-clean   {_bar(clean / n)} {clean}  (nothing in the top band)")
                print(f"  digital-at-risk {_bar(at_risk / n)} {at_risk}  (top band would be lost)")
                print(f"  unreadable      {_bar(broken / n)} {broken}")
                print(f"  projected at-risk documents: {low:.0f} to {high:.0f} of {result.pdfs} "
                      f"(95% Wilson on a {n}-document sample)")
            shown = 0
            for v in result.verdicts:
                if shown >= args.show:
                    break
                if v.kind == "digital-at-risk":
                    print(f"    at-risk: {v.name} — band text on {v.band_pages}/{v.pages} pages, "
                          f"{v.total_chars} chars total")
                    shown += 1
            for v in result.verdicts:
                if v.error is None or shown >= args.show:
                    continue
                print(f"    unreadable: {v.name}: {v.error}")
                shown += 1

    print("\n=== the number the handover decision turns on ===")
    if total_sampled:
        share = total_at_risk / total_sampled
        print(f"  {total_at_risk} of {total_sampled} sampled PDFs ({share:.0%}) carry text in "
              f"the top {args.top_band_pt:g} pt band")
        print(f"  projected over the corpus: about {share * total_pdf:.0f} of {total_pdf} PDFs "
              f"would lose their top band to a handover that overwrites Papra's text")
        print("  if that number is near zero the risk is a rounding error; if it is a few hundred, "
              "it is not")
    if pages_seen:
        print(f"  pages per document: median {statistics.median(pages_seen):.0f}, "
              f"max {max(pages_seen)}")
    print("  nothing was written to Papra, Kreuzakt was not contacted, no inference was spent")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
