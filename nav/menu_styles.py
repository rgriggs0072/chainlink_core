# nav/menu_styles.py

import streamlit as st

# Detect current Streamlit theme
theme = st.get_option("theme.base")  # returns "light" or "dark"

if theme == "dark":
    common_menu_styles = {
        "container": {
            "padding": "0!important",
            "background-color": "#1E1E1E",  # dark gray
        },
        "icon": {
            "color": "#B3D7ED",  # soft blue
            "font-size": "14px"
        },
        "nav-link": {
            "font-size": "12px",
            "color": "#FFFFFF",
            "text-align": "center",
            "margin": "0px",
            "font-weight": "500"
        },
        "nav-link-selected": {
            "background-color": "#333333",  # darker gray highlight
            "color": "#FFFFFF"
        }
    }
else:
    common_menu_styles = {
        "container": {
            "padding": "0!important",
            "background-color": "#F8F2EB",  # light beige
        },
        "icon": {
            "color": "#6497D6",  # brand blue
            "font-size": "14px"
        },
        "nav-link": {
            "font-size": "12px",
            "color": "#000000",
            "text-align": "center",
            "margin": "0px",
            "font-weight": "500"
        },
        "nav-link-selected": {
            "background-color": "#B3D7ED",  # light blue
            "color": "#000000"
        }
    }
