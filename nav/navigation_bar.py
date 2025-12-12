# ------------ navigation_bar.py ------------

import streamlit as st
from streamlit_option_menu import option_menu
from nav.menu_styles import common_menu_styles


def render_navigation(*, show_admin: bool, show_ai: bool = False) -> str:
    """
    Top nav.
      - show_admin: include Admin tab
      - show_ai: include AI & Forecasts tab
    Returns the selected label.
    """
    options = ["Home", "Reports", "Format and Upload"]
    icons   = ["house", "bar-chart", "cloud-upload"]

    if show_ai:
        options.append("AI & Forecasts")
        icons.append("stars")

    if show_admin:
        options.append("Admin")
        icons.append("gear")

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
            "nav-link": {"font-size": "12px", "text-align": "center", "margin": "0px", "color": "#000", "font-weight": "500"},
            "nav-link-selected": {"background-color": "#B3D7ED", "color": "#000"},
        }
    )
    return selected


def render_reports_submenu() -> str:
    return option_menu(
        menu_title="",
        options=["Gap Report", "Email Gap Report", "Gap History", "Data Exports"],
        icons=["file-bar-graph", "envelope", "clock-history", "file-earmark-text"],
        menu_icon="bar-chart",
        default_index=0,
        orientation="horizontal",
        styles=common_menu_styles,
    )


def render_format_upload_submenu() -> str:
    return option_menu(
        menu_title="",
        options=["Load Company Data", "Reset Schedule Processing", "Distribution Grid Processing"],
        icons=["file-earmark", "arrow-up-square", "grid-1x2"],
        menu_icon="cloud-upload",
        default_index=0,
        orientation="horizontal",
        styles=common_menu_styles,
    )


def render_ai_forecasts_submenu() -> str:
    """
    Submenu for AI & Forecasts (admin-only section).
    """
    return option_menu(
        menu_title="",
        options=["Predictive Purchases", "Predictive Truck Plan", "AI-Narrative Report", "Placement Intelligence"],
        icons=["graph-up-arrow"],
        menu_icon="stars",
        default_index=0,
        orientation="horizontal",
        styles=common_menu_styles,
    )


def render_admin_submenu() -> str:
    """
    Admin submenu using the same horizontal option_menu style
    as the AI & Forecasts submenu.
    """
    return option_menu(
        menu_title="",
        options=["Admin Dashboard", "Sales Contacts Admin"],
        icons=["gear", "envelope"],        # good icon pair; feel free to adjust
        menu_icon="tools",
        default_index=0,
        orientation="horizontal",
        styles=common_menu_styles,
    )

    return submenu
