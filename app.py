from __future__ import annotations

import json
import os
import threading
import time
from datetime import date, timedelta
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
DEFAULT_API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("API_TIMEOUT_SECONDS", "20"))
APP_TITLE = "NEET Prep Dashboard"
APP_ICON = "🧬"

TAB_ICONS = {
    "Command Center": "🏠",
    "Question Bank": "📚",
    "Practice Lab": "⚗️",
    "Analytics": "📊",
    "Mistake Journal": "📝",
    "Mastery Heatmap": "🗺️",
    "Revision": "🔁",
    "Flashcards": "🃏",
    "QOTD + Paper Builder": "🏗️",
    "Verification": "✅",
    "AI Tutor": "🤖",
}

# ─────────────────────────────────────────────────────────────
# API LAYER
# ─────────────────────────────────────────────────────────────

def _normalize_api_base(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return DEFAULT_API_BASE_URL
    return text.rstrip("/")


def _request_json(
    base_url: str,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> tuple[Any | None, str | None]:
    url = f"{_normalize_api_base(base_url)}{path}"
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            params=params,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.ConnectionError:
        return None, "Cannot reach the server. Please check your connection or API URL in Settings."
    except requests.Timeout:
        return None, "Request timed out. The server may be overloaded — please try again."
    except requests.RequestException as exc:
        return None, f"Something went wrong while connecting: {exc}"

    if not response.ok:
        detail = ""
        try:
            data = response.json()
            if isinstance(data, dict):
                detail = str(data.get("detail") or data.get("message") or "").strip()
        except ValueError:
            detail = response.text.strip()[:200]

        friendly = {
            400: "Bad request — please check your inputs.",
            401: "Authentication required.",
            403: "You don't have permission to do that.",
            404: "Resource not found.",
            422: "Invalid data sent to the server.",
            429: "Too many requests — please wait a moment.",
            500: "Server error. Please try again shortly.",
            503: "Server is unavailable. Try again later.",
        }
        msg = friendly.get(response.status_code, f"Error {response.status_code}")
        if detail:
            msg = f"{msg} ({detail})"
        return None, msg

    try:
        return response.json(), None
    except ValueError:
        snippet = response.text.strip()[:200]
        return None, f"Unexpected response format from server: {snippet}"


def _api_get(path: str, params: dict[str, Any] | None = None) -> tuple[Any | None, str | None]:
    return _request_json(st.session_state["api_base_url"], "GET", path, params=params)


def _api_post(path: str, payload: dict[str, Any] | None = None) -> tuple[Any | None, str | None]:
    return _request_json(st.session_state["api_base_url"], "POST", path, payload=payload)


@st.cache_data(ttl=120)
def _fetch_meta_options_cached(base_url: str) -> tuple[Any | None, str | None]:
    return _request_json(base_url, "GET", "/api/meta/options")


def _default_options() -> dict[str, list[Any]]:
    return {
        "subjects": [],
        "topics": [],
        "source_years": [],
        "difficulties": [],
        "question_types": [],
        "modes": ["exam", "adaptive", "omr", "pyq", "daily-quiz", "bank-practice"],
    }


def _normalize_options(payload: Any) -> dict[str, list[Any]]:
    options = _default_options()
    if not isinstance(payload, dict):
        return options
    for key in options:
        value = payload.get(key)
        if isinstance(value, list):
            options[key] = value
    return options


def _is_local_api_base(base_url: str) -> tuple[str, int] | None:
    parsed = urlparse(_normalize_api_base(base_url))
    host = str(parsed.hostname or "").strip().lower()
    if parsed.scheme != "http" or host not in {"127.0.0.1", "localhost"}:
        return None

    if parsed.port is not None:
        port = int(parsed.port)
    else:
        port = 80

    if port <= 0:
        return None
    return host, port


def _health_ok(base_url: str, timeout: float = 1.0) -> bool:
    url = f"{_normalize_api_base(base_url)}/health"
    try:
        response = requests.get(url, timeout=timeout)
        return bool(response.ok)
    except requests.RequestException:
        return False


@st.cache_resource(show_spinner=False)
def _ensure_local_backend(base_url: str) -> dict[str, str]:
    endpoint = _is_local_api_base(base_url)
    if endpoint is None:
        return {"status": "skipped", "reason": "non-local-api"}

    if _health_ok(base_url, timeout=0.8):
        return {"status": "ready", "reason": "already-running"}

    try:
        import uvicorn
        from backend.main import app as backend_app
    except Exception as exc:
        return {"status": "error", "reason": f"import-failed: {exc}"}

    host, port = endpoint
    server = uvicorn.Server(uvicorn.Config(backend_app, host=host, port=port, log_level="warning"))

    thread = threading.Thread(target=server.run, daemon=True, name="neet-fastapi")
    thread.start()

    deadline = time.time() + 20.0
    while time.time() < deadline:
        if _health_ok(base_url, timeout=0.8):
            return {"status": "ready", "reason": "started"}
        time.sleep(0.35)

    return {"status": "error", "reason": "startup-timeout"}


# ─────────────────────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────────────────────

def _init_state() -> None:
    st.session_state.setdefault("api_base_url", DEFAULT_API_BASE_URL)
    st.session_state.setdefault("active_user", "default")
    st.session_state.setdefault("practice_session", None)
    st.session_state.setdefault("practice_report", None)
    st.session_state.setdefault("verification_report", None)
    st.session_state.setdefault("question_detail_cache", {})
    st.session_state.setdefault("ai_last_reply", "")
    st.session_state.setdefault("ai_last_explanation", "")
    st.session_state.setdefault("last_mock_paper", None)
    st.session_state.setdefault("discard_confirm", False)
    st.session_state.setdefault("practice_start_time", None)
    st.session_state.setdefault("flashcard_reviewed", set())


def _clear_practice_widget_state() -> None:
    keys = [k for k in st.session_state if k.startswith("practice-choice-") or k.startswith("practice-time-")]
    for key in keys:
        del st.session_state[key]


# ─────────────────────────────────────────────────────────────
# STYLES
# ─────────────────────────────────────────────────────────────

def _inject_styles() -> None:
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=Space+Grotesk:wght@400;500;700&display=swap');

        :root {
            --bg-a: #070b17;
            --bg-b: #111a2d;
            --ink: #e7efff;
            --muted: #9db0d0;
            --brand: #ff6f3c;
            --brand-2: #2d7ff9;
            --ok: #2fd18a;
            --warn: #f5b93a;
            --bad: #ff6b6b;
            --card: rgba(11, 18, 34, 0.82);
            --line: rgba(157, 181, 219, 0.28);
        }

        html, body { font-family: 'Outfit', sans-serif; color: var(--ink); }

        [data-testid="stAppViewContainer"], [data-testid="stAppViewContainer"] * { color: var(--ink); }
        [data-testid="stSidebar"], [data-testid="stSidebar"] * { color: var(--ink); }
        h1, h2, h3, h4, h5, h6 { color: var(--ink) !important; }

        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(1200px 500px at -10% -20%, rgba(255,111,60,0.18), transparent 70%),
                radial-gradient(900px 420px at 110% -10%, rgba(45,127,249,0.24), transparent 70%),
                linear-gradient(145deg, var(--bg-a), var(--bg-b));
        }

        /* HERO */
        .app-hero {
            background: linear-gradient(135deg, rgba(255,111,60,0.95), rgba(45,127,249,0.92));
            border-radius: 18px;
            padding: 1.1rem 1.4rem;
            margin-bottom: 1.2rem;
            color: #fff;
            box-shadow: 0 18px 30px rgba(23,43,77,0.16);
            animation: slideIn 0.55s ease-out;
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 0.6rem;
        }
        .app-hero, .app-hero * { color: #fff !important; }
        .app-hero h1 {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 1.45rem;
            margin: 0 0 0.2rem 0;
            letter-spacing: 0.015em;
        }
        .app-hero p { margin: 0; opacity: 0.92; font-size: 0.96rem; }
        .hero-badge {
            background: rgba(255,255,255,0.2);
            border-radius: 50px;
            padding: 0.3rem 0.9rem;
            font-size: 0.9rem;
            font-weight: 600;
            white-space: nowrap;
        }

        /* METRICS */
        [data-testid="stMetric"] {
            border: 1px solid var(--line);
            background: var(--card);
            border-radius: 14px;
            padding: 0.5rem;
            backdrop-filter: blur(3px);
            transition: transform 160ms ease, box-shadow 160ms ease;
        }
        [data-testid="stMetric"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 22px rgba(0,0,0,0.35);
        }
        [data-testid="stMetricLabel"],
        [data-testid="stMetricValue"],
        [data-testid="stMetricDelta"] { color: var(--ink) !important; }

        .block-container { padding-top: 1.2rem; padding-bottom: 1.8rem; }

        /* SIDEBAR */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(8,14,28,0.98), rgba(12,20,39,0.94));
            border-right: 1px solid var(--line);
        }

        /* TABS */
        [data-baseweb="tab-list"] {
            gap: 0.3rem;
            border-bottom: 1px solid var(--line);
            margin-bottom: 0.8rem;
            flex-wrap: wrap;
        }
        button[data-baseweb="tab"] {
            color: var(--muted) !important;
            background: rgba(15,24,45,0.76) !important;
            border-radius: 10px 10px 0 0 !important;
            padding: 0.3rem 0.75rem !important;
            transition: all 160ms ease;
            font-size: 0.88rem !important;
        }
        button[data-baseweb="tab"]:hover {
            color: var(--ink) !important;
            background: rgba(23,36,64,0.95) !important;
        }
        button[data-baseweb="tab"][aria-selected="true"] {
            color: var(--brand) !important;
            background: rgba(14,23,43,0.98) !important;
            box-shadow: inset 0 -3px 0 0 var(--brand);
        }

        /* INPUTS */
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li,
        [data-testid="stWidgetLabel"],
        [data-testid="stCaptionContainer"] { color: var(--ink) !important; }

        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input,
        [data-testid="stTextArea"] textarea,
        [data-testid="stSelectbox"] [data-baseweb="select"] > div {
            background: rgba(11,18,35,0.94) !important;
            color: var(--ink) !important;
            border: 1px solid var(--line) !important;
        }
        [data-testid="stTextInput"] input::placeholder,
        [data-testid="stTextArea"] textarea::placeholder { color: rgba(157,176,208,0.85) !important; }

        /* BUTTONS */
        .stButton > button,
        [data-testid="baseButton-secondary"],
        [data-testid="baseButton-primary"] {
            background: linear-gradient(135deg, rgba(25,38,66,0.98), rgba(16,27,52,0.98)) !important;
            color: var(--ink) !important;
            border: 1px solid var(--line) !important;
            border-radius: 10px !important;
            transition: all 160ms ease !important;
        }
        .stButton > button:hover,
        [data-testid="baseButton-secondary"]:hover,
        [data-testid="baseButton-primary"]:hover {
            border-color: rgba(255,111,60,0.6) !important;
            box-shadow: 0 0 0 1px rgba(255,111,60,0.32), 0 8px 20px rgba(0,0,0,0.35) !important;
        }

        /* DANGER BUTTON */
        .danger-btn > button {
            border-color: rgba(255,107,107,0.5) !important;
            color: var(--bad) !important;
        }
        .danger-btn > button:hover {
            border-color: var(--bad) !important;
            box-shadow: 0 0 0 1px rgba(255,107,107,0.4) !important;
        }

        /* STATUS BADGES */
        .status-pass { color: var(--ok); font-weight: 600; }
        .status-warn { color: var(--warn); font-weight: 600; }
        .status-fail { color: var(--bad); font-weight: 600; }

        /* OPTION CARDS (radio replacement) */
        .option-card {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 10px;
            padding: 0.55rem 0.9rem;
            margin: 0.3rem 0;
            cursor: pointer;
            transition: all 140ms ease;
        }
        .option-card:hover { border-color: rgba(45,127,249,0.5); }
        .option-card-selected {
            border-color: var(--brand-2) !important;
            background: rgba(45,127,249,0.12) !important;
        }

        /* SESSION TIMER */
        .timer-bar {
            background: linear-gradient(90deg, var(--brand), var(--brand-2));
            height: 5px;
            border-radius: 99px;
            margin-bottom: 1rem;
            transition: width 1s linear;
        }

        /* FLASHCARD */
        .flash-card {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 1.2rem 1.4rem;
            margin-bottom: 0.8rem;
            backdrop-filter: blur(4px);
        }
        .flash-leech {
            border-color: rgba(255,107,107,0.5) !important;
            background: rgba(255,107,107,0.06) !important;
        }
        .flash-due {
            border-color: rgba(245,185,58,0.45) !important;
        }

        /* QUESTION CARD */
        .q-card {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 1rem 1.2rem;
            margin-bottom: 0.7rem;
        }

        /* SETTINGS PANEL */
        .settings-panel {
            background: rgba(11,18,34,0.6);
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 1rem 1.2rem;
        }

        /* SECTION DIVIDER */
        .sect-divider {
            border: none;
            border-top: 1px solid var(--line);
            margin: 1.2rem 0;
        }

        /* EMPTY STATES */
        .empty-state {
            text-align: center;
            padding: 2rem;
            color: var(--muted);
            font-size: 1rem;
        }
        .empty-state .icon { font-size: 2.5rem; margin-bottom: 0.5rem; }

        /* STREAK PILL */
        .streak-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            background: rgba(255,111,60,0.18);
            border: 1px solid rgba(255,111,60,0.35);
            border-radius: 50px;
            padding: 0.2rem 0.75rem;
            font-size: 0.9rem;
            font-weight: 600;
            color: var(--brand) !important;
        }

        /* ANSWER REVEAL */
        .answer-reveal {
            background: rgba(47,209,138,0.1);
            border: 1px solid rgba(47,209,138,0.35);
            border-radius: 10px;
            padding: 0.6rem 0.9rem;
            margin-top: 0.5rem;
        }

        @keyframes slideIn {
            from { opacity: 0; transform: translateY(8px); }
            to { opacity: 1; transform: translateY(0); }
        }
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }
        .fade-in { animation: fadeIn 0.3s ease-out; }
    </style>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _question_text(item: dict[str, Any]) -> str:
    return str(item.get("question_text") or item.get("text") or "")

def _subject_text(item: dict[str, Any]) -> str:
    return str(item.get("subject") or "Unknown")

def _topic_text(item: dict[str, Any]) -> str:
    return str(item.get("topic") or "Unknown")

def _status_badge(status: str) -> str:
    value = str(status or "").strip().lower()
    if value == "pass":
        return "<span class='status-pass'>✅ PASS</span>"
    if value == "warn":
        return "<span class='status-warn'>⚠️ WARN</span>"
    if value == "fail":
        return "<span class='status-fail'>❌ FAIL</span>"
    return str(status)

def _empty_state(icon: str, message: str) -> None:
    st.markdown(f"""
    <div class="empty-state">
        <div class="icon">{icon}</div>
        <div>{message}</div>
    </div>
    """, unsafe_allow_html=True)

def _section(title: str) -> None:
    st.markdown(f"<hr class='sect-divider'><h4 style='margin:0 0 0.6rem 0'>{title}</h4>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# USER RESOLUTION + HERO
# ─────────────────────────────────────────────────────────────

def _resolve_users_and_render_header() -> str:
    """Render the hero bar with user switcher and settings in sidebar."""
    with st.spinner("Loading…"):
        users_payload, users_error = _api_get("/api/users")

    users = ["default"]
    if not users_error and isinstance(users_payload, dict) and isinstance(users_payload.get("users"), list):
        loaded = [str(item) for item in users_payload["users"] if str(item).strip()]
        users = loaded or users

    # Sidebar: user picker + settings
    with st.sidebar:
        st.markdown(f"## {APP_ICON} {APP_TITLE}")
        st.markdown("---")

        active_user = st.session_state.get("active_user", "default")
        if active_user not in users:
            active_user = users[0]

        st.markdown("### 👤 Active Profile")
        selected_user = st.selectbox(
            "Switch user",
            users,
            index=users.index(active_user) if active_user in users else 0,
            label_visibility="collapsed",
        )
        if selected_user != st.session_state.get("active_user"):
            st.session_state["active_user"] = selected_user
            st.session_state["practice_session"] = None
            st.session_state["practice_report"] = None
            st.rerun()

        st.markdown("---")
        st.markdown("### ⚙️ Settings")
        with st.expander("API Configuration", expanded=False):
            api_url = st.text_input(
                "API Base URL",
                value=st.session_state.get("api_base_url", DEFAULT_API_BASE_URL),
                placeholder="http://127.0.0.1:8000",
            )
            if api_url.strip() != st.session_state["api_base_url"]:
                st.session_state["api_base_url"] = api_url.strip() or DEFAULT_API_BASE_URL
                st.cache_data.clear()
                st.rerun()

        st.markdown("---")
        st.caption(f"Server: `{st.session_state['api_base_url']}`")

    user_name = str(st.session_state.get("active_user", "default"))

    # Hero bar
    streak_data, _ = _api_get("/api/daily/streak", params={"user_name": user_name})
    streak_count = int(streak_data.get("streak", 0)) if isinstance(streak_data, dict) else 0
    streak_html = f"<span class='streak-pill'>🔥 {streak_count} day streak</span>" if streak_count else ""

    st.markdown(f"""
    <div class="app-hero">
        <div>
            <h1>{APP_ICON} {APP_TITLE}</h1>
            <p>Practice · Analytics · Revision · Flashcards · AI Tutoring</p>
        </div>
        <div style="display:flex;align-items:center;gap:0.7rem;flex-wrap:wrap">
            {streak_html}
            <span class="hero-badge">👤 {user_name}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    return user_name


# ─────────────────────────────────────────────────────────────
# COMMAND CENTER
# ─────────────────────────────────────────────────────────────

def _render_command_center(user_name: str) -> None:
    with st.spinner("Loading dashboard…"):
        overview, overview_error = _api_get("/api/overview")
        tagging, tagging_error = _api_get("/api/tagging-progress")
        summary, summary_error = _api_get("/api/data/summary")

    if overview_error:
        st.error(f"📡 {overview_error}")
        return

    total_questions = int(overview.get("total_questions", 0))
    tagged_questions = int(overview.get("tagged_questions", 0))
    pending_questions = int(overview.get("pending_questions", 0))
    tagged_pct = float(overview.get("tagged_pct", 0.0))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Questions", f"{total_questions:,}")
    m2.metric("Tagged", f"{tagged_questions:,}")
    m3.metric("Pending", f"{pending_questions:,}")
    m4.metric("Tagged", f"{tagged_pct:.1f}%")

    if not tagging_error and isinstance(tagging, dict):
        pct = float(tagging.get("progress_pct", 0.0))
        st.progress(min(max(pct / 100.0, 0.0), 1.0), text=f"Tagging Progress: {pct:.1f}%")

    left, right = st.columns(2)

    by_subject = pd.DataFrame(overview.get("by_subject", []))
    with left:
        _section("Subject Distribution")
        if by_subject.empty:
            _empty_state("📭", "No subject data yet")
        else:
            st.dataframe(by_subject, use_container_width=True, hide_index=True)
            chart_frame = by_subject.set_index("subject")[["total", "tagged"]]
            st.bar_chart(chart_frame)

    by_difficulty = pd.DataFrame(overview.get("by_difficulty", []))
    with right:
        _section("Difficulty Mix")
        if by_difficulty.empty:
            _empty_state("📭", "No difficulty data yet")
        else:
            st.dataframe(by_difficulty, use_container_width=True, hide_index=True)
            st.bar_chart(by_difficulty.set_index("difficulty")[["total"]])

    _section("Data Summary")
    if summary_error:
        st.warning(f"Data summary unavailable: {summary_error}")
    elif isinstance(summary, dict):
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Answer Key Coverage", int(summary.get("answer_key_coverage", 0)))
        s2.metric("Attempt Logs", int(summary.get("attempt_logs", 0)))
        s3.metric("Session Reports", int(summary.get("session_reports", 0)))
        s4.metric("Source PDFs", int(summary.get("distinct_source_pdfs", 0)))

        recent_year = pd.DataFrame(overview.get("recent_year_distribution", []))
        if not recent_year.empty:
            st.line_chart(recent_year.set_index("source_year")[["total"]])

    if not tagging_error and isinstance(tagging, dict):
        bands = pd.DataFrame(tagging.get("confidence_bands", []))
        _section("Confidence Bands")
        if bands.empty:
            _empty_state("📊", "No confidence data yet")
        else:
            st.dataframe(bands, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────
# QUESTION BANK
# ─────────────────────────────────────────────────────────────

def _render_question_bank(user_name: str, options: dict[str, list[Any]]) -> None:
    subjects = ["All"] + [str(x) for x in options.get("subjects", [])]
    topics = ["All"] + [str(x) for x in options.get("topics", [])]
    question_types = ["All"] + [str(x) for x in options.get("question_types", [])]
    difficulties = ["All"] + [str(x) for x in options.get("difficulties", [])]
    years = ["All"] + [str(x) for x in options.get("source_years", [])]

    with st.expander("🔍 Search & Filters", expanded=True):
        f1, f2, f3 = st.columns(3)
        with f1:
            st.text_input("Search text", key="bank_search_text", placeholder="Keywords…")
            st.selectbox("Subject", subjects, key="bank_subject")
        with f2:
            st.selectbox("Topic", topics, key="bank_topic")
            st.selectbox("Question Type", question_types, key="bank_question_type")
        with f3:
            st.selectbox("Difficulty", difficulties, key="bank_difficulty")
            st.selectbox("Source Year", years, key="bank_source_year")

        g1, g2, g3 = st.columns(3)
        with g1:
            st.checkbox("Only tagged", key="bank_only_tagged")
        with g2:
            st.slider("Page size", min_value=5, max_value=100, value=20, step=5, key="bank_limit")
        with g3:
            st.number_input("Page", min_value=1, value=1, step=1, key="bank_page")

    params: dict[str, Any] = {
        "limit": int(st.session_state["bank_limit"]),
        "offset": (int(st.session_state["bank_page"]) - 1) * int(st.session_state["bank_limit"]),
        "only_tagged": bool(st.session_state["bank_only_tagged"]),
    }

    search_text = str(st.session_state.get("bank_search_text", "")).strip()
    if search_text:
        params["q"] = search_text
    if st.session_state["bank_subject"] != "All":
        params["subject"] = st.session_state["bank_subject"]
    if st.session_state["bank_topic"] != "All":
        params["topic"] = st.session_state["bank_topic"]
    if st.session_state["bank_question_type"] != "All":
        params["question_type"] = st.session_state["bank_question_type"]
    if st.session_state["bank_difficulty"] != "All":
        params["difficulty"] = st.session_state["bank_difficulty"]
    if st.session_state["bank_source_year"] != "All":
        params["source_year"] = int(st.session_state["bank_source_year"])

    with st.spinner("Fetching questions…"):
        payload, error = _api_get("/api/questions", params=params)

    if error:
        st.error(f"📡 {error}")
        return

    total = int(payload.get("total", 0)) if isinstance(payload, dict) else 0
    items = payload.get("items", []) if isinstance(payload, dict) else []

    st.caption(f"**{total:,}** matched · showing **{len(items)}** · page {int(st.session_state['bank_page'])}")

    if not items:
        _empty_state("🔍", "No questions match current filters. Try broadening your search.")
        return

    for item in items:
        qid = int(item.get("id", 0))
        subject = _subject_text(item)
        topic = _topic_text(item)
        difficulty = str(item.get("difficulty") or "unknown")
        diff_emoji = {"easy": "🟢", "medium": "🟡", "hard": "🔴"}.get(difficulty.lower(), "⚪")

        with st.expander(f"{diff_emoji} Q{qid} · {subject} · {topic}"):
            st.markdown(_question_text(item))

            options_list = item.get("options", [])
            if isinstance(options_list, list) and options_list:
                st.markdown("**Options:**")
                for idx, option in enumerate(options_list, start=1):
                    st.markdown(f"&nbsp;&nbsp;**{idx}.** {option}")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Difficulty", difficulty.title())
            c2.metric("Type", str(item.get("question_type") or "—"))
            c3.metric("Year", int(item.get("source_year") or 0))
            c4.metric("Tag Conf", f"{float(item.get('tag_confidence') or 0.0):.2f}")

            detail_key = str(qid)
            if st.button("📖 Load Answer Key", key=f"load-detail-{qid}"):
                with st.spinner("Loading…"):
                    detail_payload, detail_error = _api_get(f"/api/questions/{qid}")
                if detail_error:
                    st.warning(f"Could not load answer key: {detail_error}")
                elif isinstance(detail_payload, dict):
                    st.session_state["question_detail_cache"][detail_key] = detail_payload

            cached = st.session_state["question_detail_cache"].get(detail_key)
            if isinstance(cached, dict):
                latest_answer = cached.get("latest_answer")
                if latest_answer:
                    st.markdown(f"""
                    <div class="answer-reveal">
                        ✅ <strong>Answer:</strong> Option {latest_answer.get('answer')} &nbsp;·&nbsp;
                        Source: {latest_answer.get('source') or 'unknown'}
                    </div>
                    """, unsafe_allow_html=True)
                    explanation = str(latest_answer.get("explanation") or "").strip()
                    if explanation:
                        st.caption(explanation)

            with st.form(f"attempt-form-{qid}"):
                a1, a2, a3 = st.columns(3)
                with a1:
                    selected_option = st.radio(
                        "Your answer",
                        ["Skip", "1", "2", "3", "4"],
                        horizontal=True,
                        key=f"bank-radio-{qid}",
                    )
                with a2:
                    correctness = st.selectbox("Correctness", ["Auto", "Correct", "Wrong"])
                with a3:
                    time_spent = st.number_input("Time (sec)", min_value=0, max_value=7200, value=60, step=5)
                submitted = st.form_submit_button("📝 Log Attempt", type="primary")

            if submitted:
                is_correct: bool | None = None
                if correctness == "Correct":
                    is_correct = True
                elif correctness == "Wrong":
                    is_correct = False

                with st.spinner("Logging…"):
                    _, submit_error = _api_post("/api/attempts/log", payload={
                        "user_name": user_name,
                        "mode": "bank-practice",
                        "question_id": qid,
                        "selected_option": None if selected_option == "Skip" else int(selected_option),
                        "is_correct": is_correct,
                        "time_spent_sec": int(time_spent),
                    })
                if submit_error:
                    st.error(f"Could not log attempt: {submit_error}")
                else:
                    st.success("✅ Attempt logged!")


# ─────────────────────────────────────────────────────────────
# PRACTICE LAB
# ─────────────────────────────────────────────────────────────

def _render_practice_report(report: dict[str, Any]) -> None:
    st.markdown("### 📋 Session Results")
    p1, p2, p3, p4, p5 = st.columns(5)
    p1.metric("Score", int(report.get("score", 0)))
    p2.metric("✅ Correct", int(report.get("correct", 0)))
    p3.metric("❌ Wrong", int(report.get("wrong", 0)))
    p4.metric("Accuracy", f"{float(report.get('accuracy', 0.0)):.1f}%")
    p5.metric("Attempted", int(report.get("attempted", 0)))

    details = pd.DataFrame(report.get("details", []))
    if not details.empty:
        st.dataframe(details, use_container_width=True, hide_index=True)
        st.download_button(
            label="⬇️ Download Session Report",
            data=json.dumps(report, indent=2),
            file_name=f"session-{report.get('session_id', 'report')}.json",
            mime="application/json",
        )


def _render_practice_lab(user_name: str, options: dict[str, list[Any]]) -> None:
    mode_options = [str(x) for x in options.get("modes", [])] or [
        "exam", "adaptive", "omr", "pyq", "daily-quiz", "bank-practice"
    ]
    subjects = [str(x) for x in options.get("subjects", [])]
    topics = [str(x) for x in options.get("topics", [])]
    question_types = [str(x) for x in options.get("question_types", [])]
    difficulties = [str(x) for x in options.get("difficulties", [])]
    years = [int(x) for x in options.get("source_years", []) if str(x).strip()]

    # If no active session, show the start form
    session = st.session_state.get("practice_session")

    if not isinstance(session, dict):
        with st.expander("🚀 Start New Session", expanded=True):
            with st.form("start-practice-form"):
                s1, s2, s3 = st.columns(3)
                with s1:
                    mode = st.selectbox("Mode", mode_options)
                    count = st.slider("Question count", min_value=5, max_value=180, value=45, step=5)
                with s2:
                    duration = st.slider("Duration (minutes)", min_value=10, max_value=300, value=90, step=5)
                    only_tagged = st.checkbox("Only tagged")
                with s3:
                    only_pyq = st.checkbox("Only PYQ")
                    search_text = st.text_input("Search text", placeholder="Optional keyword…")

                t1, t2 = st.columns(2)
                with t1:
                    selected_subjects = st.multiselect("Subjects", subjects)
                    selected_topics = st.multiselect("Topics", topics)
                with t2:
                    selected_types = st.multiselect("Question types", question_types)
                    selected_difficulties = st.multiselect("Difficulties", difficulties)

                selected_years = st.multiselect("Source years", years)
                start_submitted = st.form_submit_button("▶️ Start Practice", type="primary")

            if start_submitted:
                with st.spinner("Starting session…"):
                    response, error = _api_post("/api/practice/start", payload={
                        "user_name": user_name,
                        "mode": mode,
                        "count": int(count),
                        "duration_minutes": int(duration),
                        "subjects": selected_subjects,
                        "topics": selected_topics,
                        "question_types": selected_types,
                        "source_years": selected_years,
                        "difficulties": selected_difficulties,
                        "search_text": search_text,
                        "only_tagged": bool(only_tagged),
                        "only_pyq": bool(only_pyq),
                    })
                if error:
                    st.error(f"Could not start session: {error}")
                elif isinstance(response, dict):
                    st.session_state["practice_session"] = response
                    st.session_state["practice_report"] = None
                    st.session_state["practice_start_time"] = time.time()
                    _clear_practice_widget_state()
                    st.success(f"Session started! ID: {response.get('session_id')}")
                    st.rerun()

        report = st.session_state.get("practice_report")
        if isinstance(report, dict):
            _render_practice_report(report)
        return

    # Active session UI
    session_id = str(session.get("session_id") or "")
    questions = session.get("questions", [])
    question_ids = [int(q.get("id", 0)) for q in questions if int(q.get("id", 0)) > 0]
    duration_min = int(session.get("duration_minutes", 0))

    # Timer display
    start_time = st.session_state.get("practice_start_time")
    if start_time and duration_min > 0:
        elapsed_sec = int(time.time() - start_time)
        remaining_sec = max(0, duration_min * 60 - elapsed_sec)
        mins, secs = divmod(remaining_sec, 60)
        pct_done = min(elapsed_sec / (duration_min * 60), 1.0)
        timer_color = "var(--ok)" if pct_done < 0.7 else ("var(--warn)" if pct_done < 0.9 else "var(--bad)")
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:0.7rem;margin-bottom:0.6rem">
            <span style="font-size:1.1rem;font-weight:700;color:{timer_color}">⏱ {mins:02d}:{secs:02d} remaining</span>
            <div style="flex:1;background:var(--line);border-radius:99px;height:6px">
                <div style="width:{pct_done*100:.1f}%;background:{timer_color};height:6px;border-radius:99px;transition:width 1s linear"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Session", session_id[:8] + "…" if len(session_id) > 8 else session_id)
    h2.metric("Mode", str(session.get("mode") or "exam").title())
    h3.metric("Questions", len(question_ids))
    h4.metric("Duration", f"{duration_min} min")

    # Discard with confirmation
    if not st.session_state.get("discard_confirm"):
        st.markdown('<div class="danger-btn">', unsafe_allow_html=True)
        if st.button("🗑️ Discard Session"):
            st.session_state["discard_confirm"] = True
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.warning("⚠️ Are you sure? All unsaved answers will be lost.")
        dc1, dc2 = st.columns(2)
        if dc1.button("Yes, discard", type="primary"):
            st.session_state["practice_session"] = None
            st.session_state["discard_confirm"] = False
            _clear_practice_widget_state()
            st.rerun()
        if dc2.button("Cancel"):
            st.session_state["discard_confirm"] = False
            st.rerun()

    st.markdown("### 📝 Questions")
    st.caption("Questions are shown directly below for continuous reading.")

    for idx, question in enumerate(questions, start=1):
        qid = int(question.get("id", 0))
        if qid <= 0:
            continue

        choice_key = f"practice-choice-{session_id}-{qid}"
        time_key = f"practice-time-{session_id}-{qid}"

        if choice_key not in st.session_state:
            st.session_state[choice_key] = "Skip"
        if time_key not in st.session_state:
            st.session_state[time_key] = 60

        answered = st.session_state[choice_key] != "Skip"
        status_dot = "🟢" if answered else "⚪"

        with st.container():
            st.markdown(
                f"**{status_dot} Q{idx} · ID {qid} · {_subject_text(question)} · {_topic_text(question)}**"
            )
            st.markdown(_question_text(question))

            opts = question.get("options", [])
            if isinstance(opts, list) and opts:
                st.markdown("**Choose your answer:**")
                option_labels = ["Skip"] + [f"{i}. {o}" for i, o in enumerate(opts, start=1)]

                selected_display = st.radio(
                    "Answer",
                    option_labels,
                    key=f"practice-display-{session_id}-{qid}",
                    label_visibility="collapsed",
                    horizontal=False,
                )
                if selected_display == "Skip":
                    st.session_state[choice_key] = "Skip"
                else:
                    st.session_state[choice_key] = str(option_labels.index(selected_display))
            else:
                st.selectbox("Selected option", ["Skip", "1", "2", "3", "4"], key=choice_key)

            st.number_input("Time spent (sec)", min_value=0, max_value=7200, step=5, key=time_key)
            st.markdown("---")

    if st.button("✅ Submit Session", type="primary"):
        answers: dict[str, int] = {}
        time_spent_map: dict[str, int] = {}

        for qid in question_ids:
            choice_key = f"practice-choice-{session_id}-{qid}"
            time_key = f"practice-time-{session_id}-{qid}"
            selected = str(st.session_state.get(choice_key, "Skip"))
            if selected != "Skip":
                answers[str(qid)] = int(selected)
            time_spent_map[str(qid)] = int(st.session_state.get(time_key, 0) or 0)

        with st.spinner("Submitting session…"):
            report, error = _api_post("/api/practice/submit", payload={
                "user_name": user_name,
                "mode": str(session.get("mode") or "exam"),
                "session_id": session_id,
                "question_ids": question_ids,
                "answers": answers,
                "time_spent_sec": time_spent_map,
            })

        if error:
            st.error(f"Submission failed: {error}")
        elif isinstance(report, dict):
            st.session_state["practice_report"] = report
            st.session_state["practice_session"] = None
            st.session_state["practice_start_time"] = None
            _clear_practice_widget_state()
            st.success("🎉 Session submitted!")
            st.rerun()


# ─────────────────────────────────────────────────────────────
# ANALYTICS
# ─────────────────────────────────────────────────────────────

def _render_analytics(user_name: str) -> None:
    with st.spinner("Loading analytics…"):
        time_data, time_error = _api_get("/api/analytics/time", params={"user_name": user_name})
        weak_data, weak_error = _api_get("/api/analytics/weakness", params={"user_name": user_name})
        forecast_data, forecast_error = _api_get("/api/analytics/forecast", params={"user_name": user_name})
        coaching_data, coaching_error = _api_get("/api/analytics/coaching")
        share_data, share_error = _api_get("/api/daily/share-payload", params={"user_name": user_name})

    _section("Score Forecast")
    if forecast_error:
        st.warning(f"Forecast unavailable: {forecast_error}")
    elif isinstance(forecast_data, dict):
        f1, f2, f3, f4 = st.columns(4)
        f1.metric("Predicted Score", int(forecast_data.get("predicted_score", 0)))
        f2.metric("Range", f"{int(forecast_data.get('low', 0))}–{int(forecast_data.get('high', 0))}")
        f3.metric("Confidence", str(forecast_data.get("confidence") or "Unknown"))
        f4.metric("Current Accuracy", f"{float(forecast_data.get('current_accuracy', 0.0)):.1f}%")

        history = pd.DataFrame(forecast_data.get("history", []))
        if not history.empty and "submitted_at" in history.columns:
            history["submitted_at"] = pd.to_datetime(history["submitted_at"], errors="coerce")
            history = history.dropna(subset=["submitted_at"]).sort_values("submitted_at")
            if not history.empty:
                st.line_chart(history.set_index("submitted_at")[["score", "accuracy"]], use_container_width=True)

    left, right = st.columns(2)

    with left:
        _section("Time Analysis")
        if time_error:
            st.warning(f"Unavailable: {time_error}")
        elif isinstance(time_data, dict):
            st.metric("Avg time/question", f"{float(time_data.get('average_time_sec', 0.0)):.1f}s")
            by_subject = pd.DataFrame(time_data.get("by_subject", []))
            if not by_subject.empty:
                st.dataframe(by_subject, use_container_width=True, hide_index=True)
                st.bar_chart(by_subject.set_index("subject")[["avg_time_sec"]])
            by_topic = pd.DataFrame(time_data.get("by_topic", []))
            if not by_topic.empty:
                st.caption("Slowest topics:")
                st.dataframe(by_topic.head(15), use_container_width=True, hide_index=True)

    with right:
        _section("Weakness Tracker")
        if weak_error:
            st.warning(f"Unavailable: {weak_error}")
        elif isinstance(weak_data, dict):
            weak_df = pd.DataFrame(weak_data.get("items", []))
            if weak_df.empty:
                _empty_state("💪", "No weakness data yet — keep practicing!")
            else:
                st.dataframe(weak_df, use_container_width=True, hide_index=True)
                if "topic" in weak_df.columns and "accuracy" in weak_df.columns:
                    st.bar_chart(weak_df.set_index("topic")[["accuracy"]])
            plan_df = pd.DataFrame(weak_data.get("recovery_plan_7d", []))
            if not plan_df.empty:
                st.caption("7-day recovery plan:")
                st.dataframe(plan_df, use_container_width=True, hide_index=True)

    _section("Coaching Dashboard")
    if coaching_error:
        st.warning(f"Unavailable: {coaching_error}")
    elif isinstance(coaching_data, dict):
        coaching_df = pd.DataFrame(coaching_data.get("items", []))
        if coaching_df.empty:
            _empty_state("🎓", "No coaching data yet")
        else:
            st.dataframe(coaching_df, use_container_width=True, hide_index=True)

    _section("Daily Share")
    if share_error:
        st.warning(f"Share payload unavailable: {share_error}")
    elif isinstance(share_data, dict):
        msg = str(share_data.get("message") or "")
        st.text_area("Today's share message", value=msg, height=90)
        if msg:
            st.button("📋 Copy to clipboard", help="Paste into WhatsApp, Telegram, etc.")


# ─────────────────────────────────────────────────────────────
# MISTAKE JOURNAL
# ─────────────────────────────────────────────────────────────

def _render_mistake_journal(user_name: str) -> None:
    limit = st.slider("Journal rows", min_value=20, max_value=300, value=120, step=10)

    with st.spinner("Loading mistake journal…"):
        payload, error = _api_get("/api/mistakes/journal", params={"user_name": user_name, "limit": int(limit)})

    if error:
        st.error(f"📡 {error}")
        return

    if not isinstance(payload, dict):
        _empty_state("📝", "No mistakes logged yet. Keep practicing!")
        return

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Mistakes", int(payload.get("total_logged_mistakes", 0)))
    m2.metric("Top Root Cause", str(payload.get("top_root_cause") or "—"))
    m3.metric("Weak Topics", len(payload.get("weak_topics", [])))

    cause_df = pd.DataFrame(payload.get("root_cause_summary", []))
    if not cause_df.empty:
        _section("Root Cause Breakdown")
        left, right = st.columns(2)
        with left:
            st.dataframe(cause_df, use_container_width=True, hide_index=True)
        with right:
            if "mistake_type" in cause_df.columns and "count" in cause_df.columns:
                st.bar_chart(cause_df.set_index("mistake_type")[["count"]])

    weak_df = pd.DataFrame(payload.get("weak_topics", []))
    if not weak_df.empty:
        _section("Weak Topics by Wrong Count")
        st.dataframe(weak_df, use_container_width=True, hide_index=True)

    rows_df = pd.DataFrame(payload.get("items", []))
    if not rows_df.empty:
        _section("Recent Wrong Attempts")
        st.dataframe(rows_df, use_container_width=True, hide_index=True)

    recommendations = payload.get("recommendations", [])
    if recommendations:
        _section("💡 Suggested Recovery Actions")
        for tip in recommendations:
            st.info(f"➡️ {tip}")


# ─────────────────────────────────────────────────────────────
# MASTERY HEATMAP
# ─────────────────────────────────────────────────────────────

def _render_mastery_heatmap(user_name: str) -> None:
    min_attempts = st.slider("Min attempts per topic", min_value=1, max_value=10, value=2, step=1)

    with st.spinner("Building heatmap…"):
        payload, error = _api_get(
            "/api/analytics/mastery-heatmap",
            params={"user_name": user_name, "min_attempts": int(min_attempts)},
        )

    if error:
        st.error(f"📡 {error}")
        return
    if not isinstance(payload, dict):
        _empty_state("🗺️", "No mastery data yet. Submit more sessions to see your heatmap.")
        return

    items_df = pd.DataFrame(payload.get("items", []))
    if items_df.empty:
        _empty_state("🗺️", "Solve and submit more tests to populate this heatmap.")
        return

    st.metric("Topic cells", len(items_df))

    pivot = items_df.pivot_table(index="subject", columns="topic", values="mastery_score", aggfunc="mean")
    _section("Heatmap Matrix")
    st.dataframe(
        pivot.style.background_gradient(cmap="RdYlGn", axis=None).format("{:.1f}"),
        use_container_width=True,
    )

    weak_df = pd.DataFrame(payload.get("weakest_topics", []))
    strong_df = pd.DataFrame(payload.get("strongest_topics", []))
    left, right = st.columns(2)
    with left:
        _section("📉 Weakest Topics")
        if weak_df.empty:
            _empty_state("👍", "No weak topics identified yet")
        else:
            st.dataframe(weak_df, use_container_width=True, hide_index=True)
    with right:
        _section("📈 Strongest Topics")
        if strong_df.empty:
            _empty_state("🌱", "No strong topics tracked yet")
        else:
            st.dataframe(strong_df, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────
# REVISION
# ─────────────────────────────────────────────────────────────

def _render_revision(user_name: str) -> None:
    with st.expander("📅 Generate New Plan", expanded=False):
        with st.form("revision-generate-form"):
            r1, r2 = st.columns(2)
            with r1:
                days = st.slider("Plan duration (days)", min_value=7, max_value=180, value=30, step=1)
            with r2:
                daily_target = st.slider("Daily question target", min_value=20, max_value=200, value=60, step=5)
            weak_topics_text = st.text_input("Weak topics (comma separated, optional)", placeholder="e.g. Thermodynamics, Genetics")
            generate = st.form_submit_button("🗓️ Generate Plan", type="primary")

    if generate:
        weak_topics = [c.strip() for c in weak_topics_text.split(",") if c.strip()]
        with st.spinner("Generating revision plan…"):
            response, error = _api_post("/api/revision/generate", payload={
                "user_name": user_name,
                "days": int(days),
                "daily_question_target": int(daily_target),
                "weak_topics": weak_topics,
            })
        if error:
            st.error(f"Could not generate plan: {error}")
        elif isinstance(response, dict):
            st.success(f"✅ Generated {int(response.get('generated_days', 0))} days of revision!")
            st.rerun()

    with st.spinner("Loading revision plan…"):
        payload, error = _api_get("/api/revision/plan", params={"user_name": user_name})

    if error:
        st.error(f"📡 {error}")
        return

    items = payload.get("items", []) if isinstance(payload, dict) else []
    completion_pct = float(payload.get("completion_pct", 0.0)) if isinstance(payload, dict) else 0.0

    st.progress(min(completion_pct / 100.0, 1.0), text=f"Plan completion: {completion_pct:.1f}%")

    if not items:
        _empty_state("📅", "No revision plan found. Generate one above.")
        return

    table_rows = []
    for row in items:
        tasks = row.get("tasks", [])
        tasks_text = " · ".join(str(x) for x in tasks) if isinstance(tasks, list) else str(tasks)
        completed = bool(row.get("completed"))
        table_rows.append({
            "Date": row.get("date"),
            "Topic": row.get("topic"),
            "Done": "✅" if completed else "⏳",
            "Tasks": tasks_text,
        })

    st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

    _section("Update a Day")
    dates = [str(item.get("date")) for item in items]
    u1, u2, u3 = st.columns(3)
    with u1:
        selected_date = st.selectbox("Plan date", dates)
    with u2:
        mark_state = st.selectbox("Set status", ["Completed", "Pending"])
    with u3:
        st.write("")
        st.write("")
        update_clicked = st.button("💾 Update", type="primary")

    if update_clicked:
        with st.spinner("Updating…"):
            _, mark_error = _api_post("/api/revision/mark", payload={
                "user_name": user_name,
                "plan_date": selected_date,
                "completed": mark_state == "Completed",
            })
        if mark_error:
            st.error(f"Could not update: {mark_error}")
        else:
            st.success("✅ Updated!")
            st.rerun()


# ─────────────────────────────────────────────────────────────
# FLASHCARDS
# ─────────────────────────────────────────────────────────────

def _review_flashcard(user_name: str, question_id: int, rating: str) -> None:
    with st.spinner(f"Marking as {rating}…"):
        _, error = _api_post("/api/flashcards/review", payload={
            "user_name": user_name,
            "question_id": int(question_id),
            "rating": str(rating),
        })
    if error:
        st.error(f"Review failed: {error}")
    else:
        st.session_state["flashcard_reviewed"].add(question_id)
        st.success(f"Card {question_id} marked as **{rating}** ✓")
        st.rerun()


def _render_flashcards(user_name: str) -> None:
    with st.expander("➕ Generate Flashcards from Wrong Attempts", expanded=False):
        with st.form("generate-flashcards-form"):
            generate_limit = st.number_input("Generate limit", min_value=10, max_value=1000, value=200, step=10)
            generate = st.form_submit_button("⚡ Generate Cards")

    if generate:
        with st.spinner("Generating flashcards…"):
            response, error = _api_post("/api/flashcards/generate", payload={
                "user_name": user_name, "limit": int(generate_limit)
            })
        if error:
            st.error(f"Generation failed: {error}")
        elif isinstance(response, dict):
            st.success(f"✅ Added **{int(response.get('added', 0))}** new card(s). Total: {int(response.get('total_cards', 0))}")

    c1, c2, c3 = st.columns(3)
    with c1:
        due_only = st.checkbox("Due only", value=True)
    with c2:
        leech_only = st.checkbox("Leech only", value=False)
    with c3:
        limit = st.slider("Fetch limit", min_value=10, max_value=200, value=60, step=10)

    with st.spinner("Loading flashcards…"):
        payload, error = _api_get("/api/flashcards", params={
            "user_name": user_name,
            "due_only": due_only,
            "leech_only": leech_only,
            "limit": int(limit),
        })

    if error:
        st.error(f"📡 {error}")
        return

    total = int(payload.get("total", 0)) if isinstance(payload, dict) else 0
    due_today = int(payload.get("due_today", 0)) if isinstance(payload, dict) else 0
    leech_cards = int(payload.get("leech_cards", 0)) if isinstance(payload, dict) else 0
    items = payload.get("items", []) if isinstance(payload, dict) else []

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Cards", total)
    m2.metric("Due Today", due_today, delta=f"{due_today} pending" if due_today else None)
    m3.metric("Leech Cards", leech_cards, delta="Need attention" if leech_cards else None, delta_color="inverse")

    if not items:
        _empty_state("🃏", "No flashcards found. Generate cards from wrong attempts above.")
        return

    reviewed = st.session_state.get("flashcard_reviewed", set())

    for idx, item in enumerate(items, start=1):
        qid = int(item.get("question_id", 0))
        is_leech = bool(item.get("is_leech", False))
        is_reviewed = qid in reviewed

        next_due = item.get("next_due", "—")
        subject = str(item.get("subject") or "Unknown")
        topic = str(item.get("topic") or "Unknown")

        leech_flag = " 🔴 LEECH" if is_leech else ""
        done_flag = " ✅" if is_reviewed else ""
        card_header = f"Card {idx} · Q{qid} · {subject} · {topic}{leech_flag}{done_flag}"

        card_class = "flash-leech" if is_leech else ("flash-due" if due_only else "")

        with st.expander(card_header):
            st.markdown(str(item.get("question_text") or ""))

            col_a, col_b, col_c, col_d = st.columns(4)
            col_a.caption(f"📅 Due: {next_due}")
            col_b.caption(f"🔄 Interval: {item.get('interval_days', '—')}d")
            col_c.caption(f"😌 Ease: {item.get('ease', '—')}")
            col_d.caption(f"💔 Lapses: {item.get('lapse_count', 0)} · Reviews: {item.get('review_count', 0)}")

            if is_leech:
                st.warning("🔴 **Leech card** — this topic has repeated lapses and needs focused attention.")

            st.markdown(f"""
            <div class="answer-reveal">
                📌 <strong>Answer:</strong> Option {item.get('answer_key', '—')}
            </div>
            """, unsafe_allow_html=True)

            st.markdown("**How did you do?**")
            r1, r2, r3 = st.columns(3)
            if r1.button("😵 Again", key=f"flash-again-{idx}-{qid}", help="Didn't remember"):
                _review_flashcard(user_name, qid, "again")
            if r2.button("👍 Good", key=f"flash-good-{idx}-{qid}", help="Remembered with effort"):
                _review_flashcard(user_name, qid, "good")
            if r3.button("⚡ Easy", key=f"flash-easy-{idx}-{qid}", help="Remembered instantly"):
                _review_flashcard(user_name, qid, "easy")


# ─────────────────────────────────────────────────────────────
# QOTD + PAPER BUILDER
# ─────────────────────────────────────────────────────────────

def _parse_paper_sections(raw_text: str) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for line in str(raw_text or "").splitlines():
        text = line.strip()
        if not text:
            continue
        parts = [chunk.strip() for chunk in text.split(",")]
        if len(parts) < 3:
            continue
        try:
            count = int(parts[2])
        except Exception:
            continue
        section: dict[str, Any] = {
            "name": parts[0] or "Section",
            "subject": None if parts[1] in {"", "*"} else parts[1],
            "count": max(1, count),
        }
        if len(parts) >= 4 and parts[3] not in {"", "*"}:
            section["topic"] = parts[3]
        if len(parts) >= 5 and parts[4] not in {"", "*"}:
            section["question_type"] = parts[4]
        sections.append(section)
    return sections


def _render_qotd_and_paper_builder(user_name: str, options: dict[str, list[Any]]) -> None:
    _section("🧪 Question of the Day")

    with st.spinner("Loading QOTD…"):
        qotd_payload, qotd_error = _api_get("/api/qotd", params={"user_name": user_name})

    if qotd_error:
        st.error(f"📡 {qotd_error}")
    elif isinstance(qotd_payload, dict):
        question = qotd_payload.get("question", {}) if isinstance(qotd_payload.get("question"), dict) else {}
        qid = int(question.get("id", 0) or 0)
        if qid > 0:
            with st.expander(f"Today's Question · Q{qid} · {_subject_text(question)} · {_topic_text(question)}", expanded=True):
                st.markdown(_question_text(question))

                opts = question.get("options", [])
                if isinstance(opts, list) and opts:
                    for idx, option in enumerate(opts, start=1):
                        st.markdown(f"**{idx}.** {option}")

                attempted = bool(qotd_payload.get("attempted_today", False))
                if attempted:
                    attempt = qotd_payload.get("attempt", {}) if isinstance(qotd_payload.get("attempt"), dict) else {}
                    is_correct = attempt.get("is_correct")
                    result_icon = "✅" if is_correct else "❌"
                    st.success(f"{result_icon} Already submitted · Your answer: **{attempt.get('selected_option')}** · Correct: **{qotd_payload.get('correct_option')}**")
                else:
                    with st.form("qotd-submit-form"):
                        selected_option = st.radio("Your answer", ["1", "2", "3", "4"], horizontal=True)
                        time_spent = st.number_input("Time spent (sec)", min_value=0, max_value=7200, value=60, step=5)
                        submit_qotd = st.form_submit_button("📤 Submit Answer", type="primary")

                    if submit_qotd:
                        with st.spinner("Submitting…"):
                            response, submit_error = _api_post("/api/qotd/submit", payload={
                                "user_name": user_name,
                                "question_id": qid,
                                "selected_option": int(selected_option),
                                "time_spent_sec": int(time_spent),
                            })
                        if submit_error:
                            st.error(f"Submit failed: {submit_error}")
                        elif isinstance(response, dict):
                            correct = response.get("correct_option")
                            is_right = str(selected_option) == str(correct)
                            if is_right:
                                st.success(f"🎉 Correct! The answer is **{correct}**.")
                            else:
                                st.error(f"❌ Not quite. The correct answer is **{correct}**.")
                            st.rerun()

    _section("🏗️ Mock Test Paper Builder")
    subjects = [str(x) for x in options.get("subjects", [])]
    topics = [str(x) for x in options.get("topics", [])]
    question_types = [str(x) for x in options.get("question_types", [])]
    difficulties = [str(x) for x in options.get("difficulties", [])]
    source_years = [int(x) for x in options.get("source_years", []) if str(x).strip()]

    with st.form("mock-paper-builder-form"):
        b1, b2, b3 = st.columns(3)
        with b1:
            title = st.text_input("Paper title", value="NEET Full Mock")
            total_questions = st.slider("Total questions", min_value=30, max_value=240, value=180, step=5)
        with b2:
            duration_minutes = st.slider("Duration (minutes)", min_value=30, max_value=360, value=180, step=5)
            only_tagged = st.checkbox("Only tagged", value=True)
        with b3:
            only_pyq = st.checkbox("Only PYQ", value=False)

        f1, f2 = st.columns(2)
        with f1:
            selected_subjects = st.multiselect("Subjects", subjects)
            selected_topics = st.multiselect("Topics", topics)
            selected_question_types = st.multiselect("Question types", question_types)
        with f2:
            selected_difficulties = st.multiselect("Difficulties", difficulties)
            selected_years = st.multiselect("Source years", source_years)
            sections_text = st.text_area(
                "Sections (Name,Subject,Count per line)",
                value="Physics,Physics,45\nChemistry,Chemistry,45\nBiology,Biology,90",
                height=110,
            )

        build_paper = st.form_submit_button("🏗️ Build Paper", type="primary")

    if build_paper:
        sections = _parse_paper_sections(sections_text)
        with st.spinner("Building paper…"):
            response, build_error = _api_post("/api/mock-paper/build", payload={
                "user_name": user_name,
                "title": str(title or "Custom Mock Paper"),
                "total_questions": int(total_questions),
                "duration_minutes": int(duration_minutes),
                "subjects": selected_subjects,
                "topics": selected_topics,
                "question_types": selected_question_types,
                "source_years": selected_years,
                "difficulties": selected_difficulties,
                "only_tagged": bool(only_tagged),
                "only_pyq": bool(only_pyq),
                "sections": sections,
            })
        if build_error:
            st.error(f"Build failed: {build_error}")
        elif isinstance(response, dict):
            st.session_state["last_mock_paper"] = response
            st.success(f"✅ Built paper **#{response.get('paper_id')}** with **{response.get('total_questions')}** questions!")

    with st.spinner("Loading saved papers…"):
        list_payload, list_error = _api_get("/api/mock-paper/list", params={"user_name": user_name, "limit": 25})

    if list_error:
        st.warning(f"Could not load saved papers: {list_error}")
    elif isinstance(list_payload, dict):
        items = list_payload.get("items", [])
        if items:
            labels = [f"#{item.get('paper_id')} · {item.get('title')} · {item.get('created_at')}" for item in items]
            lc1, lc2 = st.columns([4, 1])
            with lc1:
                selected_label = st.selectbox("Load saved paper", labels)
            with lc2:
                st.write("")
                st.write("")
                if st.button("📂 Load"):
                    selected_id = int(str(selected_label).split("·", 1)[0].replace("#", "").strip())
                    with st.spinner("Loading paper…"):
                        paper_detail, detail_error = _api_get(f"/api/mock-paper/{selected_id}")
                    if detail_error:
                        st.error(f"Could not load: {detail_error}")
                    elif isinstance(paper_detail, dict):
                        st.session_state["last_mock_paper"] = paper_detail

    paper = st.session_state.get("last_mock_paper")
    if isinstance(paper, dict):
        _section("📄 Latest Built Paper")
        m1, m2, m3 = st.columns(3)
        m1.metric("Paper ID", int(paper.get("paper_id", 0)))
        m2.metric("Questions", int(paper.get("total_questions", 0)))
        m3.metric("Duration", f"{int(paper.get('duration_minutes', 0))} min")

        section_df = pd.DataFrame(paper.get("sections", []))
        if not section_df.empty:
            st.markdown("#### Sections")
            st.dataframe(section_df, use_container_width=True, hide_index=True)

        question_df = pd.DataFrame(paper.get("questions", []))
        if not question_df.empty:
            cols = [c for c in ["id", "subject", "topic", "question_type", "difficulty", "source_year"] if c in question_df.columns]
            st.markdown("#### Questions")
            st.dataframe(question_df[cols], use_container_width=True, hide_index=True)

        st.download_button(
            label="⬇️ Download Paper JSON",
            data=json.dumps(paper, indent=2),
            file_name=f"mock-paper-{paper.get('paper_id', 'latest')}.json",
            mime="application/json",
        )


# ─────────────────────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────────────────────

def _render_verification() -> None:
    b1, b2 = st.columns(2)
    if b1.button("📸 Quick Snapshot"):
        with st.spinner("Running snapshot…"):
            report, error = _api_get("/api/verification/snapshot")
        if error:
            st.error(f"Snapshot failed: {error}")
        elif isinstance(report, dict):
            st.session_state["verification_report"] = report
            st.success("Snapshot complete!")

    with b2.form("full-verification-form"):
        deep_pdf_scan = st.checkbox("Deep PDF scan", value=False)
        verify_remote_sources = st.checkbox("Verify remote sources", value=False)
        pdf_sample_limit = st.slider("PDF sample limit", min_value=5, max_value=200, value=30, step=5)
        remote_sample_limit = st.slider("Remote sample limit", min_value=0, max_value=200, value=25, step=5)
        remote_timeout_seconds = st.slider("Remote timeout (sec)", min_value=5, max_value=120, value=20, step=5)
        run_full = st.form_submit_button("🔍 Run Full Verification")

    if run_full:
        with st.spinner("Running full verification (this may take a while)…"):
            report, error = _api_post("/api/verification/run", payload={
                "deep_pdf_scan": bool(deep_pdf_scan),
                "verify_remote_sources": bool(verify_remote_sources),
                "pdf_sample_limit": int(pdf_sample_limit),
                "remote_sample_limit": int(remote_sample_limit),
                "remote_timeout_seconds": int(remote_timeout_seconds),
            })
        if error:
            st.error(f"Verification failed: {error}")
        elif isinstance(report, dict):
            st.session_state["verification_report"] = report
            st.success("Verification complete!")

    report = st.session_state.get("verification_report")
    if not isinstance(report, dict):
        _empty_state("✅", "Run a snapshot or full verification to see results here.")
        return

    status = str(report.get("status") or "unknown")
    st.markdown(f"**Overall Status:** {_status_badge(status)}", unsafe_allow_html=True)

    m1, m2, m3 = st.columns(3)
    m1.metric("✅ Passed", int(report.get("passed", 0)))
    m2.metric("⚠️ Warnings", int(report.get("warnings", 0)))
    m3.metric("❌ Failed", int(report.get("failed", 0)))

    checks = report.get("checks", [])
    rows = [{"Name": c.get("name"), "Status": str(c.get("status") or "").upper(), "Message": c.get("message")} for c in checks]
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    for check in checks:
        name = str(check.get("name") or "Check")
        status_text = str(check.get("status") or "").upper()
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(status_text, "❓")
        with st.expander(f"{icon} {name} [{status_text}]"):
            st.write(str(check.get("message") or ""))
            st.json(check.get("metrics") or {})

    st.download_button(
        label="⬇️ Download Verification Report",
        data=json.dumps(report, indent=2),
        file_name="verification-report.json",
        mime="application/json",
    )


# ─────────────────────────────────────────────────────────────
# AI TUTOR
# ─────────────────────────────────────────────────────────────

def _render_ai_tutor(user_name: str) -> None:
    st.info("💡 The backend must have `GROQ_API_KEY` configured for AI features to work.")

    _section("💬 Ask the AI Tutor")
    with st.form("ai-ask-form"):
        prompt = st.text_area("Your question", height=120, placeholder="e.g. Explain the mechanism of enzyme inhibition…")
        context = st.text_area("Extra context (optional)", height=80, placeholder="Paste relevant question text or notes…")
        ask = st.form_submit_button("🚀 Ask Tutor", type="primary")

    if ask:
        if not prompt.strip():
            st.warning("Please enter a question first.")
        else:
            with st.spinner("Thinking…"):
                response, error = _api_post("/api/ai/ask", payload={
                    "user_name": user_name,
                    "prompt": str(prompt or ""),
                    "context": str(context or ""),
                })
            if error:
                st.error(f"AI request failed: {error}")
            elif isinstance(response, dict):
                st.session_state["ai_last_reply"] = str(response.get("reply") or "")

    if st.session_state.get("ai_last_reply"):
        st.markdown("#### 🤖 Tutor's Reply")
        st.markdown(st.session_state["ai_last_reply"])
        if st.button("🗑️ Clear reply"):
            st.session_state["ai_last_reply"] = ""
            st.rerun()

    _section("🔍 Explain a Specific Question")
    with st.form("ai-explain-form"):
        ec1, ec2 = st.columns(2)
        with ec1:
            qid = st.number_input("Question ID", min_value=1, value=1, step=1)
        with ec2:
            selected_option = st.selectbox("Your answer (optional)", ["Unknown", "1", "2", "3", "4"])
        explain = st.form_submit_button("💡 Generate Explanation", type="primary")

    if explain:
        with st.spinner("Generating explanation…"):
            response, error = _api_post("/api/ai/explain", payload={
                "user_name": user_name,
                "question_id": int(qid),
                "selected_option": None if selected_option == "Unknown" else int(selected_option),
            })
        if error:
            st.error(f"Explanation failed: {error}")
        elif isinstance(response, dict):
            st.session_state["ai_last_explanation"] = str(response.get("explanation") or "")
            correct = response.get("correct_option")
            if correct:
                st.markdown(f"""
                <div class="answer-reveal">
                    ✅ <strong>Official answer:</strong> Option {correct}
                </div>
                """, unsafe_allow_html=True)

    if st.session_state.get("ai_last_explanation"):
        st.markdown("#### 📖 Explanation")
        st.markdown(st.session_state["ai_last_explanation"])


# ─────────────────────────────────────────────────────────────
# GOALS & RANK (bonus tab)
# ─────────────────────────────────────────────────────────────

def _render_goals_and_rank(user_name: str) -> None:
    with st.spinner("Loading goal data…"):
        goal_payload, goal_error = _api_get("/api/goals/current", params={"user_name": user_name})

    if goal_error:
        st.error(f"📡 {goal_error}")
        return
    if not isinstance(goal_payload, dict):
        _empty_state("🎯", "No goal data available.")
        return

    exam_date_text = str(goal_payload.get("exam_date") or (date.today() + timedelta(days=120)).isoformat())
    try:
        exam_date_value = date.fromisoformat(exam_date_text)
    except Exception:
        exam_date_value = date.today() + timedelta(days=120)

    with st.form("goal-planner-form"):
        g1, g2, g3 = st.columns(3)
        with g1:
            target_score = st.number_input("Target score", min_value=100, max_value=720, value=int(goal_payload.get("target_score", 650)), step=5)
        with g2:
            exam_date = st.date_input("Exam date", value=exam_date_value)
        with g3:
            daily_goal = st.number_input("Daily question goal", min_value=10, max_value=300, value=int(goal_payload.get("daily_question_goal", 60)), step=5)
        save_goal = st.form_submit_button("💾 Save Goal", type="primary")

    if save_goal:
        with st.spinner("Saving…"):
            _, set_error = _api_post("/api/goals/set", payload={
                "user_name": user_name,
                "target_score": int(target_score),
                "exam_date": exam_date.isoformat(),
                "daily_question_goal": int(daily_goal),
            })
        if set_error:
            st.error(f"Could not save goal: {set_error}")
        else:
            st.success("✅ Goal saved!")
            st.rerun()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Predicted Score", float(goal_payload.get("predicted_score", 0.0)))
    m2.metric("Target Score", int(goal_payload.get("target_score", 650)))
    m3.metric("Gap", float(goal_payload.get("target_gap", 0.0)))
    m4.metric("Days Left", int(goal_payload.get("days_left", 0)))

    n1, n2 = st.columns(2)
    n1.metric("Required Accuracy", f"{float(goal_payload.get('required_accuracy', 0.0)):.1f}%")
    n2.metric("Daily Q Goal", int(goal_payload.get("daily_question_goal", 0)))

    milestones_df = pd.DataFrame(goal_payload.get("weekly_milestones", []))
    if not milestones_df.empty:
        _section("Weekly Milestones")
        st.dataframe(milestones_df, use_container_width=True, hide_index=True)
        if "week" in milestones_df.columns and "target_score" in milestones_df.columns:
            st.line_chart(milestones_df.set_index("week")[["target_score"]], use_container_width=True)

    with st.spinner("Loading rank projection…"):
        rank_payload, rank_error = _api_get("/api/analytics/rank-projection", params={"user_name": user_name})

    _section("🏆 Rank Projection")
    if rank_error:
        st.warning(f"Rank projection unavailable: {rank_error}")
    elif isinstance(rank_payload, dict):
        projected = rank_payload.get("projected", {}) or {}
        low_band = rank_payload.get("low_band", {}) or {}
        high_band = rank_payload.get("high_band", {}) or {}

        r1, r2, r3 = st.columns(3)
        r1.metric("Estimated Rank", int(projected.get("estimated_rank", 0)))
        r2.metric("Percentile", f"{float(projected.get('estimated_percentile', 0.0)):.3f}")
        r3.metric("Confidence", str(rank_payload.get("confidence") or "Low"))
        st.caption(f"Rank band: {int(high_band.get('estimated_rank', 0))} — {int(low_band.get('estimated_rank', 0))}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon=APP_ICON,
        layout="wide",
        initial_sidebar_state="expanded",
    )

    _init_state()

    backend_bootstrap = _ensure_local_backend(st.session_state["api_base_url"])
    if backend_bootstrap.get("status") == "error":
        st.warning(
            "Local API auto-start failed. Start backend manually with "
            "`uvicorn backend.main:app --host 127.0.0.1 --port 8000` "
            "or set API Base URL in Settings."
        )

    _inject_styles()

    user_name = _resolve_users_and_render_header()

    with st.spinner("Loading options…"):
        options_payload, options_error = _fetch_meta_options_cached(st.session_state["api_base_url"])
    options = _normalize_options(options_payload)
    if options_error:
        st.warning(f"⚠️ Could not load filter options from server. Using defaults.")

    tab_labels = [f"{TAB_ICONS[name]} {name}" for name in TAB_ICONS]
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        _render_command_center(user_name)
    with tabs[1]:
        _render_question_bank(user_name, options)
    with tabs[2]:
        _render_practice_lab(user_name, options)
    with tabs[3]:
        _render_analytics(user_name)
    with tabs[4]:
        _render_mistake_journal(user_name)
    with tabs[5]:
        _render_mastery_heatmap(user_name)
    with tabs[6]:
        _render_revision(user_name)
    with tabs[7]:
        _render_flashcards(user_name)
    with tabs[8]:
        _render_qotd_and_paper_builder(user_name, options)
    with tabs[9]:
        _render_verification()
    with tabs[10]:
        _render_ai_tutor(user_name)


if __name__ == "__main__":
    main()
