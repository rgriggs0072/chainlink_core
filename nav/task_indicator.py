# nav/task_indicator.py
"""
Task Indicator — two-location alert system:
  1. Bold full-width colored bar above the nav menu
  2. Sidebar card below the welcome message

Usage in chainlink_core.py:

    from nav.task_indicator import render_task_indicator, render_task_sidebar_card

    # 1) Call this BEFORE render_navigation() in main()
    render_task_indicator(conn=st.session_state["conn"],
                          tenant_id=st.session_state["tenant_id"])

    # 2) Call this INSIDE render_sidebar_header(), inside the `with st.sidebar:` block
    render_task_sidebar_card(conn=st.session_state["conn"],
                             tenant_id=st.session_state["tenant_id"])
"""

import streamlit as st
from datetime import date, timedelta


# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────
_CSS = """
<style>
/* ── Top bar ── */
.cl-taskbar-wrap {
    width: 100%;
    margin-bottom: 0;
}
.cl-taskbar-ok {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 10px;
    padding: 7px 24px;
    background: #d4edda;
    border-bottom: 2px solid #a8d5b5;
    font-family: 'DM Mono', 'Courier New', monospace;
    font-size: 12px;
    font-weight: 600;
    color: #1a5c32;
    letter-spacing: 0.2px;
}
.cl-taskbar-warn {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 9px 24px;
    background: #fff3cd;
    border-bottom: 3px solid #f5a623;
    font-family: 'DM Mono', 'Courier New', monospace;
}
.cl-taskbar-warn-left {
    display: flex;
    align-items: center;
    gap: 10px;
    font-size: 13px;
    font-weight: 700;
    color: #7d4e00;
}
.cl-taskbar-warn-right {
    font-size: 11px;
    color: #a06000;
}
.cl-taskbar-dot-ok   { width:8px;height:8px;border-radius:50%;background:#28a745;display:inline-block;flex-shrink:0; }
.cl-taskbar-dot-warn { width:8px;height:8px;border-radius:50%;background:#f5a623;display:inline-block;flex-shrink:0;animation:cl-blink 1.5s infinite; }
@keyframes cl-blink { 0%,100%{opacity:1} 50%{opacity:0.2} }

.cl-taskbar-tasks {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
}
.cl-taskbar-chip {
    background: #fde8a0;
    border: 1px solid #f5c842;
    border-radius: 4px;
    padding: 2px 10px;
    font-size: 11px;
    font-weight: 600;
    color: #7d4e00;
}
.cl-taskbar-chip-done {
    background: #d4edda;
    border: 1px solid #a8d5b5;
    border-radius: 4px;
    padding: 2px 10px;
    font-size: 11px;
    font-weight: 600;
    color: #1a5c32;
    text-decoration: line-through;
    opacity: 0.7;
}
.cl-taskbar-chip-blocked {
    background: #f0f0f0;
    border: 1px solid #cccccc;
    border-radius: 4px;
    padding: 2px 10px;
    font-size: 11px;
    font-weight: 500;
    color: #888888;
    opacity: 0.6;
}

/* ── Sidebar card ── */
.cl-sidebar-card-ok {
    background: #eaf7ee;
    border: 1px solid #a8d5b5;
    border-left: 4px solid #28a745;
    border-radius: 8px;
    padding: 12px 14px;
    margin: 12px 0 4px;
    font-family: 'DM Mono', 'Courier New', monospace;
}
.cl-sidebar-card-warn {
    background: #fff8e6;
    border: 1px solid #fde68a;
    border-left: 4px solid #f5a623;
    border-radius: 8px;
    padding: 12px 14px;
    margin: 12px 0 4px;
    font-family: 'DM Mono', 'Courier New', monospace;
}
.cl-sidebar-card-title {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 6px;
}
.cl-sidebar-card-ok   .cl-sidebar-card-title { color: #1a5c32; }
.cl-sidebar-card-warn .cl-sidebar-card-title { color: #7d4e00; }

.cl-sidebar-task-row {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    margin-bottom: 6px;
    font-size: 11px;
    color: #7d4e00;
    line-height: 1.4;
}
.cl-sidebar-task-row:last-child { margin-bottom: 0; }
.cl-sidebar-task-icon { flex-shrink: 0; font-size: 13px; }

.cl-sidebar-ok-text {
    font-size: 11px;
    color: #276749;
    line-height: 1.4;
}
</style>
"""


# ─────────────────────────────────────────────────────────────────────────────
# Shared task status fetch (cached 5 min)
# ─────────────────────────────────────────────────────────────────────────────
def _week_start() -> date:
    today = date.today()
    return today - timedelta(days=today.weekday())


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_task_statuses(_conn_id: str, tenant_id: str) -> dict:
    conn = st.session_state.get("conn")
    if not conn:
        return {"error": True}

    monday   = _week_start()
    statuses = {}

    # Task 1: sales report uploaded this week
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(LAST_LOAD_DATE)
                FROM   SALES_REPORT
                WHERE  TENANT_ID = %s
            """, (tenant_id,))
            row = cur.fetchone()
        last_load = row[0] if row else None
        if hasattr(last_load, "date"):
            last_load = last_load.date()
        statuses["sales_uploaded"] = bool(last_load and last_load >= monday)
        statuses["last_load_date"] = last_load
    except Exception:
        statuses["sales_uploaded"] = False
        statuses["last_load_date"] = None

    # Task 2: weekly snapshot published
    try:
        from utils.gap_snapshot_pipeline import fetch_snapshot_status
        import pandas as pd
        df  = fetch_snapshot_status(conn, tenant_id)
        col = "SNAPSHOT_WEEK_START"
        if df.empty or col not in df.columns:
            statuses["snapshot_done"] = False
        else:
            df[col] = pd.to_datetime(df[col]).dt.date
            statuses["snapshot_done"] = bool((df[col] >= monday).any())
    except Exception:
        statuses["snapshot_done"] = False

    return statuses


def _maybe_auto_snapshot(conn, tenant_id: str) -> None:
    """
    Agent: if sales report uploaded this week and snapshot not yet published,
    auto-trigger publish_weekly_snapshot_all() once per session.

    Uses st.session_state as a guard so it only fires once per login session,
    even as the user navigates between pages.
    """
    guard_key = f"agent_snapshot_fired_{tenant_id}"
    if st.session_state.get(guard_key):
        return  # already attempted this session

    try:
        from utils.gap_snapshot_pipeline import publish_weekly_snapshot_all
        success, msg = publish_weekly_snapshot_all(
            conn=conn,
            tenant_id=int(tenant_id),
            triggered_by="agent:upload_detected",
        )
        st.session_state[guard_key]             = True
        st.session_state["agent_snapshot_ok"]   = success
        st.session_state["agent_snapshot_msg"]  = msg

        if success:
            # Bust the 5-min cache so task bar reflects the new snapshot immediately
            _fetch_task_statuses.clear()
    except Exception as e:
        st.session_state[guard_key]            = True
        st.session_state["agent_snapshot_ok"]  = False
        st.session_state["agent_snapshot_msg"] = str(e)


def _get_tasks(conn, tenant_id: str) -> tuple[list[dict], dict]:
    """
    Returns (tasks_due list, raw statuses dict).

    Always returns BOTH tasks so users see the full weekly checklist.
    Tasks show as pending/blocked/done based on current state.

    Agent logic:
      If sales report uploaded but snapshot not yet published,
      auto-triggers publish_weekly_snapshot_all() once per session.
    """
    statuses  = _fetch_task_statuses(str(id(conn)), str(tenant_id))
    tasks_due = []

    if statuses.get("error"):
        return tasks_due, statuses

    sales_ok    = statuses.get("sales_uploaded", False)
    snapshot_ok = statuses.get("snapshot_done", False)
    last_load   = statuses.get("last_load_date")
    last_str    = last_load.strftime("%b %d") if last_load else "never"

    # ── Agent: auto-publish snapshot if upload detected ───────────────────
    if sales_ok and not snapshot_ok:
        _maybe_auto_snapshot(conn, tenant_id)
        # Re-fetch after agent run in case snapshot is now done
        statuses    = _fetch_task_statuses(str(id(conn)), str(tenant_id))
        snapshot_ok = statuses.get("snapshot_done", False)

    # ── All done → return empty list (hides top bar, sidebar shows green) ─
    if sales_ok and snapshot_ok:
        return [], statuses

    # ── Task 1: Upload sales report ───────────────────────────────────────
    if not sales_ok:
        tasks_due.append({
            "icon":  "📂",
            "text":  "Upload weekly sales report",
            "sub":   f"Last upload: {last_str}",
            "state": "pending",
        })
    else:
        tasks_due.append({
            "icon":  "✅",
            "text":  "Upload weekly sales report",
            "sub":   f"Uploaded {last_str}",
            "state": "done",
        })

    # ── Task 2: Publish weekly snapshot ───────────────────────────────────
    if not sales_ok:
        # Blocked — upload must happen first
        tasks_due.append({
            "icon":  "🔒",
            "text":  "Publish weekly snapshot",
            "sub":   "Waiting on sales report upload",
            "state": "blocked",
        })
    elif not snapshot_ok:
        agent_msg = st.session_state.get("agent_snapshot_msg", "")
        tasks_due.append({
            "icon":  "📌",
            "text":  "Publish weekly snapshot",
            "sub":   f"Agent error — publish manually ({agent_msg})" if agent_msg else "Agent will auto-publish on next load",
            "state": "pending",
        })

    return tasks_due, statuses


# ─────────────────────────────────────────────────────────────────────────────
# 1) TOP BAR  — call before render_navigation() in chainlink_core.py
# ─────────────────────────────────────────────────────────────────────────────
def render_task_indicator(conn, tenant_id: str) -> None:
    """
    Renders the full-width amber bar above the nav menu.
    Hidden completely when all tasks are complete — sidebar handles the green state.
    """
    if not conn or not tenant_id:
        return

    tasks_due, _ = _get_tasks(conn, tenant_id)

    # All done — top bar stays hidden, sidebar shows green
    if not tasks_due:
        return

    # Count only non-done tasks for the badge number
    pending = [t for t in tasks_due if t.get("state") != "done"]
    count   = len(pending)
    label   = f"⚠️ &nbsp; {count} TASK{'S' if count > 1 else ''} DUE THIS WEEK"

    chips_html = ""
    for t in tasks_due:
        if t.get("state") == "done":
            chip_cls = "cl-taskbar-chip-done"
        elif t.get("state") == "blocked":
            chip_cls = "cl-taskbar-chip-blocked"
        else:
            chip_cls = "cl-taskbar-chip"
        chips_html += f'<span class="{chip_cls}">{t["icon"]} {t["text"]}</span>'

    st.markdown(_CSS, unsafe_allow_html=True)
    st.markdown(f"""
    <div class="cl-taskbar-wrap">
        <div class="cl-taskbar-warn">
            <div class="cl-taskbar-warn-left">
                <span class="cl-taskbar-dot-warn"></span>
                {label}
                <div class="cl-taskbar-tasks">{chips_html}</div>
            </div>
            <div class="cl-taskbar-warn-right">
                Action required to keep reports current
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# 2) SIDEBAR CARD — uses native Streamlit components (no unsafe_allow_html)
# Streamlit sidebar blocks custom HTML reliably; native widgets always render.
# ─────────────────────────────────────────────────────────────────────────────
def render_task_sidebar_card(conn, tenant_id: str) -> None:
    """
    Always visible in sidebar.
    - Tasks pending: shows amber warning + full checklist with both tasks
    - All complete: shows quiet green success message only
    """
    if not conn or not tenant_id:
        return

    tasks_due, _ = _get_tasks(conn, tenant_id)

    st.sidebar.markdown(
        "<div style='margin:-8px 0 -8px 0'><hr style='border-color:#e0d8d0;margin:0'/></div>",
        unsafe_allow_html=True,
    )

    if not tasks_due:
        # All done — quiet green, no clutter
        st.sidebar.success("✅ **Weekly Tasks Complete**")
    else:
        pending_count = len([t for t in tasks_due if t.get("state") != "done"])
        st.sidebar.warning(f"⚠️ **{pending_count} Task{'s' if pending_count > 1 else ''} Due This Week**")
        for t in tasks_due:
            icon  = t["icon"]
            text  = t["text"]
            sub   = t["sub"]
            state = t.get("state", "pending")
            if state == "done":
                st.sidebar.markdown(f"~~{icon} {text}~~  \n*{sub}*")
            elif state == "blocked":
                st.sidebar.markdown(f"*{icon} {text}*  \n*{sub}*")
            else:
                st.sidebar.markdown(f"**{icon} {text}**  \n*{sub}*")

    st.sidebar.markdown(
        "<div style='margin:-8px 0 -8px 0'><hr style='border-color:#e0d8d0;margin:0'/></div>",
        unsafe_allow_html=True,
    )
