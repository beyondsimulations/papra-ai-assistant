from papra_suite.pipeline import enqueue_ocr_job
from papra_suite.store import Store


def test_enqueue_ocr_job_creates_seen_document_and_job(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        job_id = enqueue_ocr_job(
            store, papra_org_id="org_1", document_id="doc_1", file_hash="abc123",
            name="bill.pdf", mime="application/pdf", ingested_via="mail",
        )
        assert store.document_seen("org_1", "doc_1")
        job = store.get_job(job_id)
        assert job.kind == "ocr" and job.status == "pending"
        assert job.content_hash == "abc123"
        assert job.payload == {"file_hash": "abc123", "name": "bill.pdf"}
    finally:
        store.close()


def test_enqueue_ocr_job_is_idempotent(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        first = enqueue_ocr_job(
            store, papra_org_id="org_1", document_id="doc_1", file_hash="abc123",
            name="bill.pdf", mime="application/pdf", ingested_via="mail",
        )
        second = enqueue_ocr_job(
            store, papra_org_id="org_1", document_id="doc_1", file_hash="abc123",
            name="bill.pdf", mime="application/pdf", ingested_via="sweep",
        )
        assert first == second
        assert len(store.list_jobs()) == 1
    finally:
        store.close()


def test_enqueue_ocr_job_new_hash_creates_new_job(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        a = enqueue_ocr_job(store, papra_org_id="org_1", document_id="doc_1",
                           file_hash="h1", name="n", mime="application/pdf",
                           ingested_via="mail")
        b = enqueue_ocr_job(store, papra_org_id="org_1", document_id="doc_1",
                           file_hash="h2", name="n", mime="application/pdf",
                           ingested_via="mail")
        assert a != b
    finally:
        store.close()
