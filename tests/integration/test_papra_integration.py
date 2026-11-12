import pytest

from papra_suite.papra_client import PapraClient

pytestmark = pytest.mark.integration

SEARCH_PHRASE = "quokka-marmalade-protocol"  # occurs only in content, never in the filename


async def test_full_document_round_trip(papra_stack):
    base_url, org_id, api_key = papra_stack
    import fitz  # dev dep in M1; runtime dep from M3 on

    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((72, 72), "Rechnung 42")
    pdf_bytes = pdf.tobytes()
    pdf.close()

    async with PapraClient(base_url, api_key) as client:
        # upload
        document = await client.create_document(org_id, "rechnung-42.pdf", pdf_bytes)
        document_id = document["id"]
        assert document["name"] == "rechnung-42.pdf"

        # metadata
        fetched = await client.get_document(org_id, document_id)
        assert fetched["id"] == document_id

        # write back name + content (one endpoint serves both)
        updated = await client.update_document(
            org_id, document_id,
            name="Rechnung 42 – Titel",
            content=f"# Rechnung 42\n\n{SEARCH_PHRASE}",
        )
        assert updated["name"] == "Rechnung 42 – Titel"

        # search must find the content-only phrase
        documents, count = await client.list_documents(org_id, search_query=SEARCH_PHRASE)
        assert count >= 1
        assert document_id in {doc["id"] for doc in documents}

        # original file survives content write-back
        downloaded = await client.download_file(org_id, document_id)
        assert downloaded == pdf_bytes

        # tags: create via a raw httpx call, then attach.
        # NOTE: the brief encoded this as form-data; the live API only accepts
        # JSON here (form-data returns 415 api.unsupported-content-type).
        import httpx

        async with httpx.AsyncClient(base_url=base_url) as http:
            tag = await http.post(
                f"/api/organizations/{org_id}/tags",
                json={"name": "suite-test", "color": "#000000"},
                headers={"Authorization": f"Bearer {api_key}"},
            )
            tag.raise_for_status()
            tag_id = tag.json()["tag"]["id"]

        tags = await client.list_tags(org_id)
        assert tag_id in {tag["id"] for tag in tags}
        await client.add_tag(org_id, document_id, tag_id)
