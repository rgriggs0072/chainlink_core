# ------------ navigation_bar.py ------------

from streamlit_option_menu import option_menu
from nav.menu_styles import common_menu_styles

def render_navigation(*, show_admin: bool) -> str:
    """
    Top nav. Pass show_admin=True to include the Admin tab.
    Returns the selected label.
    """
    options = ["Home", "Reports", "Format and Upload"]
    icons   = ["house", "bar-chart", "cloud-upload"]

    if show_admin:
        options.append("Admin")
        icons.append("gear")

    # pick a safe default index (0 = Home) regardless of whether Admin is present
    selected = option_menu(
        menu_title=None,
        options=options,
        icons=icons,
        menu_icon="cast",
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "background-color": "#F8F2EB", "justify-content": "left"},
            "icon": {"color": "#6497D6", "font-size": "14px"},
            "nav-link": {"font-size": "12px", "text-align": "center", "margin": "0px", "color": "#000000", "font-weight": "500"},
            "nav-link-selected": {"background-color": "#B3D7ED", "color": "#000000"},
        }
    )
    return selected


def render_reports_submenu() -> str:
    submenu = option_menu(
        menu_title="",
        options=["Gap Report", "Email Gap Report", "Data Exports", "AI-Narrative Report", "Placement Intelligence"],
        icons=["file-bar-graph", "envelope", "file-earmark-text", "file-text", "search"],
        menu_icon="bar-chart",
        default_index=0,
        orientation="horizontal",
        styles=common_menu_styles,
    )
    return submenu


def render_format_upload_submenu() -> str:
    submenu = option_menu(
        menu_title="",
        options=["Load Company Data", "Reset Schedule Processing", "Distribution Grid Processing"],
        icons=["file-earmark", "arrow-up-square", "grid-1x2"],
        menu_icon="cloud-upload",
        default_index=0,
        orientation="horizontal",
        styles=common_menu_styles,
    )
    return submenu
