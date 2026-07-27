# nav/navigation_bar.py
"""
Sidebar navigation with collapsible sub-menus.
Replaces the two-row horizontal nav with a single unified sidebar.
"""

import streamlit as st
from streamlit_option_menu import option_menu
from utils.logout_utils import handle_logout
from utils.ui_helpers import add_logo
from nav.task_indicator import render_task_sidebar_card

# ── Brand colors ──────────────────────────────────────────────────────────────
PRIMARY   = "#6497D6"
SECONDARY = "#B3D7ED"
BG        = "#FFFFFF"
BG2       = "#F1F5F9"
TEXT      = "#1E293B"
TEXT_MUTE = "#64748B"

# ── Navigation structure ──────────────────────────────────────────────────────
NAV_SECTIONS = {
    "Home": {
        "icon": "house-fill",
        "sub_pages": [],
    },
    "Chat": {
        "icon": "chat-dots-fill",
        "sub_pages": [],
    },
    "Reports": {
        "icon": "bar-chart-fill",
        "sub_pages": [
            ("Gap Report",       "file-bar-graph"),
            ("Email Gap Report", "envelope"),
            ("Data Exports",     "file-earmark-arrow-down"),
        ],
    },
    "Format & Upload": {
        "icon": "cloud-upload-fill",
        "sub_pages": [
            ("Load Company Data",              "file-earmark"),
            ("Reset Schedule Processing",      "arrow-repeat"),
            ("Distribution Grid Processing",   "grid-1x2"),
        ],
    },
    "Documents": {
        "icon": "journal-text",
        "sub_pages": [],
    },
    "AI & Forecasts": {
        "icon": "stars",
        "sub_pages": [
            ("Predictive Purchases",   "graph-up-arrow"),
            ("Predictive Truck Plan",  "truck"),
            ("AI-Narrative Report",    "file-text"),
            ("Placement Intelligence", "grid"),
            ("Data Query",             "search"),
        ],
    },
    "Admin": {
        "icon": "gear-fill",
        "sub_pages": [
            ("Admin Dashboard",      "speedometer2"),
            ("Sales Contacts Admin", "person-badge"),
        ],
    },
}

_SIDEBAR_CSS = """
<style>
/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #F1F5F9 !important;
    border-right: 1px solid #E2E8F0;
}
section[data-testid="stSidebar"] > div { padding-top: 0 !important; }

/* Brand header */
.cl-brand { text-align: center; padding: 1.25rem 0 0.75rem; }
.cl-brand h2 {
    color: #6497D6; font-size: 1.05rem; font-weight: 700;
    margin: 6px 0 2px; letter-spacing: 0.5px;
}
.cl-brand p { color: #64748B; font-size: 0.7rem; margin: 0; letter-spacing: 2px; }

/* Welcome strip */
.cl-welcome {
    background: #6497D6; color: white; border-radius: 8px;
    padding: 0.5rem 0.75rem; margin: 0.5rem 0.75rem;
    font-size: 0.85rem; font-weight: 600;
}

/* Footer */
.cl-footer {
    font-size: 0.65rem; color: #94A3B8;
    text-align: center; padding: 0.5rem;
}

/* Tab styling — brand colors */
.stTabs [data-baseweb="tab-list"] {
    gap: 6px;
    background-color: #FFFFFF;
    border-bottom: 2px solid #B3D7ED;
    padding-bottom: 0;
}
.stTabs [data-baseweb="tab"] {
    background-color: transparent;
    border-radius: 8px 8px 0 0;
    color: #1E293B;
    font-weight: 500;
    font-size: 0.875rem;
    padding: 8px 18px;
    border: 1px solid transparent;
    border-bottom: none;
}
.stTabs [data-baseweb="tab"]:hover {
    background-color: #EFF6FF;
    color: #6497D6;
}
.stTabs [aria-selected="true"] {
    background-color: #6497D6 !important;
    color: white !important;
    font-weight: 600 !important;
    border-color: #B3D7ED !important;
}
.stTabs [data-baseweb="tab-panel"] {
    padding-top: 1.25rem;
}

/* Hide default menu/footer */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
</style>
"""


def _main_menu_styles(is_sub: bool = False) -> dict:
    return {
        "container": {
            "padding": "0 !important",
            "background-color": BG,
            "margin": "0",
        },
        "icon": {
            "color": PRIMARY,
            "font-size": "13px" if not is_sub else "11px",
        },
        "nav-link": {
            "font-size": "13px" if not is_sub else "12px",
            "font-weight": "600" if not is_sub else "400",
            "color": TEXT,
            "border-radius": "8px" if not is_sub else "6px",
            "padding": "0.45rem 0.75rem" if not is_sub else "0.3rem 0.6rem",
            "margin": "2px 0" if not is_sub else "1px 0",
            "--hover-color": SECONDARY,
        },
        "nav-link-selected": {
            "background-color": PRIMARY if not is_sub else SECONDARY,
            "color": "white" if not is_sub else TEXT,
            "font-weight": "700" if not is_sub else "600",
        },
    }


def _section_default_index(sections: list[str]) -> int:
    current = st.session_state.get("nav_section", "Home")
    return sections.index(current) if current in sections else 0


def _sub_default_index(sub_options: list[str]) -> int:
    current = st.session_state.get("nav_sub_page")
    return sub_options.index(current) if current in sub_options else 0


def render_sidebar_navigation(
    *,
    display_name: str,
    tenant_config: dict,
    authenticator,
    show_admin: bool,
    show_ai: bool,
    app_version: str = "",
    app_env: str = "local",
    conn=None,
    tenant_id: str = "",
) -> str:
    """
    Renders the sidebar: logo, welcome, section nav, logout, version.
    Sub-pages are handled by st.tabs() in the main content area.
    Returns the selected section name.
    """
    st.markdown(_SIDEBAR_CSS, unsafe_allow_html=True)

    sections = ["Home", "Chat", "Reports", "Format & Upload", "Documents"]
    if show_ai:
        sections.append("AI & Forecasts")
    if show_admin:
        sections.append("Admin")

    section_icons = [NAV_SECTIONS[s]["icon"] for s in sections]

    if st.session_state.get("nav_section") not in sections:
        st.session_state["nav_section"] = "Home"

    with st.sidebar:
        # ── Brand header ──────────────────────────────────────────────────────
        logo_path = tenant_config.get("logo_path", "")
        image = add_logo(logo_path, width=140)

        st.markdown('<div class="cl-brand">', unsafe_allow_html=True)
        if image:
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.image(image, width=140)
        else:
            st.markdown(
                f'<div style="width:56px;height:56px;border-radius:14px;'
                f'background:linear-gradient(135deg,{PRIMARY},{SECONDARY});'
                f'display:flex;align-items:center;justify-content:center;'
                f'margin:0 auto;font-size:1.75rem;">CL</div>',
                unsafe_allow_html=True,
            )
        st.markdown(
            '<h2>Chainlink Analytics</h2><p>POWERED BY AI</p></div>',
            unsafe_allow_html=True,
        )

        # ── Welcome strip ─────────────────────────────────────────────────────
        first = (display_name or "User").split()[0]
        st.markdown(
            f'<div class="cl-welcome">Welcome, {first}!</div>',
            unsafe_allow_html=True,
        )

        st.markdown("<hr style='border-color:#D5CEC4;margin:0.75rem 0'>", unsafe_allow_html=True)

        # ── Section navigation ────────────────────────────────────────────────
        selected_section = option_menu(
            menu_title=None,
            options=sections,
            icons=section_icons,
            default_index=_section_default_index(sections),
            orientation="vertical",
            styles=_main_menu_styles(is_sub=False),
            key="main_nav_menu",
        )

        st.session_state["nav_section"] = selected_section

        # ── Task sidebar card ─────────────────────────────────────────────────
        render_task_sidebar_card(conn=conn, tenant_id=tenant_id)

        # ── Logout & footer ───────────────────────────────────────────────────
        st.markdown("<hr style='border-color:#D5CEC4;margin:0.75rem 0'>", unsafe_allow_html=True)
        handle_logout(authenticator)

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            f'<div class="cl-footer">'
            f'{"v" + app_version + "<br>" if app_version else ""}'
            f'© 2026 Chainlink Analytics LLC</div>',
            unsafe_allow_html=True,
        )

    return selected_section


# ── Login page branding ───────────────────────────────────────────────────────

def render_login_branding():
    """Render Chainlink branding and center the login form + messages."""
    import os

    st.markdown(f"""
    <style>
    /* Center and constrain the authenticator form */
    div[data-testid="stForm"] {{
        max-width: 420px;
        margin: 0 auto;
        background: white;
        padding: 2rem;
        border-radius: 12px;
        border: 1px solid #D5CEC4;
        box-shadow: 0 4px 20px rgba(100,151,214,0.12);
    }}
    /* Hide the default "Login" form title injected by streamlit-authenticator */
    div[data-testid="stForm"] h1,
    div[data-testid="stForm"] h2,
    div[data-testid="stForm"] h3 {{
        display: none !important;
    }}
    /* Center warning / info / success messages on login page */
    div[data-testid="stAlert"] {{
        max-width: 420px;
        margin: 0.5rem auto;
    }}
    /* Center expander (Forgot password) */
    div[data-testid="stExpander"] {{
        max-width: 420px;
        margin: 0.5rem auto;
    }}
    .login-title {{
        text-align: center;
        color: {PRIMARY};
        font-size: 1.6rem;
        font-weight: 700;
        margin: 0.75rem 0 0.25rem;
    }}
    .login-sub {{
        text-align: center;
        color: {TEXT_MUTE};
        font-size: 0.75rem;
        letter-spacing: 3px;
        margin-bottom: 1.5rem;
    }}
    </style>
    """, unsafe_allow_html=True)

    # Logo — base64 embedded so CSS centering works reliably
    import base64
    logo_path = os.path.join(os.getcwd(), "images", "Default_Logo", "default_logo.png")
    try:
        with open(logo_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        ext = logo_path.rsplit(".", 1)[-1].lower()
        mime = "image/jpeg" if ext in ("jpg", "jpeg") else "image/png"
        logo_html = f'<img src="data:{mime};base64,{img_b64}" width="200" style="margin:0 auto 0.75rem;display:block;">'
    except Exception:
        logo_html = (
            f'<div style="width:64px;height:64px;border-radius:16px;'
            f'background:linear-gradient(135deg,{PRIMARY},{SECONDARY});'
            f'display:flex;align-items:center;justify-content:center;'
            f'margin:0 auto 0.75rem;color:white;font-weight:700;font-size:1.2rem;">CL</div>'
        )

    st.markdown(f"""
    <div style="text-align:center; padding-top: 2rem;">
        {logo_html}
        <p class="login-title">Chainlink Analytics</p>
        <p class="login-sub">SIGN IN TO YOUR ACCOUNT</p>
    </div>
    """, unsafe_allow_html=True)
