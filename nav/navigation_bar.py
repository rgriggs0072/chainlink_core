# ------------ navigation_bar.py ------------

from streamlit_option_menu import option_menu

from nav.menu_styles import common_menu_styles


def render_navigation():
    selected = option_menu(
        menu_title=None,
        options=[
            "Home",
            "Reports",
            "Format and Upload",
            "Admin"
        ],
        icons=["house", "bar-chart", "cloud-upload", "gear"],
        menu_icon="cast",
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {
                "padding": "0!important",
                "background-color": "#F8F2EB",
                "justify-content": "left"
                # "flex-wrap": "wrap" ❌ REMOVE THIS — it breaks horizontal layout
            },
            "icon": {"color": "#6497D6", "font-size": "14px"},
            "nav-link": {
                "font-size": "12px",
                "text-align": "center",
                "margin": "0px",
                "color": "#000000",
                "font-weight": "500"
            },
            "nav-link-selected": {
                "background-color": "#B3D7ED",
                "color": "#000000"
            },
        }
    )
    return selected


def render_reports_submenu():
    submenu = option_menu(
        menu_title="",
        options=["Gap Report", "Data Exports", "AI-Narrative Report", "Placement Intelligence"],
        icons=["file-bar-graph", "file-spreadsheet", "file-earmark-text","search"],
        menu_icon="bar-chart",
        default_index=0,
        orientation="horizontal",
        styles=common_menu_styles
    )
    return submenu

def render_format_upload_submenu():
    submenu = option_menu(
        menu_title="",
        options=[
            "Load Company Data",
            "Reset Schedule Processing",
            "Distribution Grid Processing"
        ],
        icons=["file-earmark", "file-arrow-up", "file-bar-graph"],
        menu_icon="cloud-upload",
        default_index=0,
        orientation="horizontal",
        styles=common_menu_styles
    )
    return submenu
