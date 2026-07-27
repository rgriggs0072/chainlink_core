# ---------------- documents.py ----------------
"""
Documents & Resources page.

Fetches document metadata from the platform-level DOCUMENTS table
(TENANTUSERDB.CHAINLINK_SCH — see utils/documents_helpers.py), groups
by CATEGORY, and renders download buttons for each PDF served from
/resources/docs/. Metadata is dynamic (Snowflake); PDF files are
static, committed to the repo.
"""

from pathlib import Path

import streamlit as st

from utils.documents_helpers import fetch_documents

DOCS_DIR = Path(__file__).parent.parent / "resources" / "docs"


@st.cache_data(ttl=3600, show_spinner=False)
def _read_pdf_bytes(file_path: str) -> bytes | None:
    """Read a PDF's bytes from disk, cached. Returns None if missing."""
    path = Path(file_path)
    if not path.is_file():
        return None
    return path.read_bytes()


def render():
    st.title("📚 Documents & Resources")
    st.markdown("Download guides and how-to's for using Chainlink Analytics.")

    conn = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")
    if not conn or not tenant_id:
        st.error("Database connection not found.")
        return

    try:
        documents = fetch_documents(conn, tenant_id)
    except Exception as e:
        st.error(f"Could not load documents: {e}")
        return

    if not documents:
        st.info("No documents available yet.")
        return

    # documents is already ordered by CATEGORY, DISPLAY_ORDER — group
    # while preserving that order rather than re-sorting categories
    # alphabetically, so DISPLAY_ORDER-driven category order is respected.
    categories: dict[str, list[dict]] = {}
    for doc in documents:
        categories.setdefault(doc["category"], []).append(doc)

    for category, docs_in_category in categories.items():
        st.subheader(category)

        for doc in docs_in_category:
            file_path = DOCS_DIR / doc["file_name"]
            pdf_bytes = _read_pdf_bytes(str(file_path))

            if pdf_bytes is None:
                print(f"[documents] Skipping '{doc['title']}' — file not found: {file_path}")
                continue

            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**{doc['title']}**")
                if doc["description"]:
                    st.caption(doc["description"])
            with col2:
                st.download_button(
                    label="Download PDF",
                    data=pdf_bytes,
                    file_name=doc["file_name"],
                    mime="application/pdf",
                    width='stretch',
                    key=f"doc_download_{doc['file_name']}",
                )

        st.divider()
