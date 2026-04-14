from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import hashlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any
from uuid import uuid4

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from verification import run_project_verification

try:
    from groq import Groq
except Exception:
    Groq = None

APP_TITLE = "NEET Learning Platform API"
DB_PATH = Path(os.getenv("DB_PATH", "data/db/questions.db"))
DEFAULT_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]
DEFAULT_AI_MODEL = os.getenv("AI_GROQ_MODEL", "llama-3.3-70b-versatile")
ALLOWED_SUBJECTS: tuple[str, ...] = ("Biology", "Botany", "Physics", "Zoology")
_ALLOWED_SUBJECT_LOOKUP = {item.lower(): item for item in ALLOWED_SUBJECTS}
_ALLOWED_SUBJECTS_LOWER: tuple[str, ...] = tuple(_ALLOWED_SUBJECT_LOOKUP.keys())

AUX_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ui_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name       TEXT NOT NULL,
    mode            TEXT NOT NULL,
    session_id      TEXT,
    question_id     INTEGER NOT NULL,
    selected_option INTEGER,
    correct_option  INTEGER,
    is_correct      INTEGER,
    time_spent_sec  INTEGER DEFAULT 0,
    mistake_type    TEXT,
    attempted_at    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ui_attempts_user_name ON ui_attempts(user_name);
CREATE INDEX IF NOT EXISTS idx_ui_attempts_mode ON ui_attempts(mode);
CREATE INDEX IF NOT EXISTS idx_ui_attempts_question_id ON ui_attempts(question_id);
CREATE INDEX IF NOT EXISTS idx_ui_attempts_attempted_at ON ui_attempts(attempted_at);

CREATE TABLE IF NOT EXISTS ui_sessions (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name                   TEXT NOT NULL,
    session_id                  TEXT NOT NULL,
    mode                        TEXT NOT NULL,
    total_questions             INTEGER DEFAULT 0,
    attempted                   INTEGER DEFAULT 0,
    graded                      INTEGER DEFAULT 0,
    correct                     INTEGER DEFAULT 0,
    wrong                       INTEGER DEFAULT 0,
    score                       INTEGER DEFAULT 0,
    accuracy                    REAL DEFAULT 0.0,
    avg_time_per_question_sec   REAL DEFAULT 0.0,
    submitted_at                TEXT DEFAULT (datetime('now')),
    UNIQUE(user_name, session_id, mode)
);

CREATE INDEX IF NOT EXISTS idx_ui_sessions_user_name ON ui_sessions(user_name);
CREATE INDEX IF NOT EXISTS idx_ui_sessions_mode ON ui_sessions(mode);
CREATE INDEX IF NOT EXISTS idx_ui_sessions_submitted_at ON ui_sessions(submitted_at);

CREATE TABLE IF NOT EXISTS ui_revision_plan (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name   TEXT NOT NULL,
    plan_date   TEXT NOT NULL,
    topic       TEXT NOT NULL,
    tasks       TEXT NOT NULL,
    completed   INTEGER DEFAULT 0,
    created_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(user_name, plan_date)
);

CREATE INDEX IF NOT EXISTS idx_ui_revision_user_date ON ui_revision_plan(user_name, plan_date);

CREATE TABLE IF NOT EXISTS ui_flashcards (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name       TEXT NOT NULL,
    question_id     INTEGER NOT NULL,
    interval_days   INTEGER DEFAULT 1,
    ease            REAL DEFAULT 2.3,
    next_due        TEXT NOT NULL,
    last_reviewed   TEXT,
    UNIQUE(user_name, question_id)
);

CREATE INDEX IF NOT EXISTS idx_ui_flashcards_user_due ON ui_flashcards(user_name, next_due);

CREATE TABLE IF NOT EXISTS ui_goals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name           TEXT NOT NULL UNIQUE,
    target_score        INTEGER NOT NULL,
    exam_date           TEXT NOT NULL,
    daily_question_goal INTEGER DEFAULT 60,
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ui_qotd_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name       TEXT NOT NULL,
    qotd_date       TEXT NOT NULL,
    question_id     INTEGER NOT NULL,
    selected_option INTEGER,
    correct_option  INTEGER,
    is_correct      INTEGER,
    time_spent_sec  INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(user_name, qotd_date)
);

CREATE INDEX IF NOT EXISTS idx_ui_qotd_user_date ON ui_qotd_log(user_name, qotd_date);

CREATE TABLE IF NOT EXISTS ui_mock_papers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name           TEXT NOT NULL,
    title               TEXT NOT NULL,
    total_questions     INTEGER DEFAULT 0,
    duration_minutes    INTEGER DEFAULT 180,
    config_json         TEXT NOT NULL,
    question_ids_json   TEXT NOT NULL,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ui_mock_papers_user_created ON ui_mock_papers(user_name, created_at DESC);
"""


class PracticeStartRequest(BaseModel):
    user_name: str = "default"
    mode: str = "exam"
    count: int = Field(default=60, ge=1, le=300)
    duration_minutes: int = Field(default=180, ge=5, le=300)
    subjects: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    question_types: list[str] = Field(default_factory=list)
    source_years: list[int] = Field(default_factory=list)
    difficulties: list[str] = Field(default_factory=list)
    search_text: str = ""
    only_tagged: bool = False
    only_pyq: bool = False


class PracticeSubmitRequest(BaseModel):
    user_name: str = "default"
    mode: str = "exam"
    session_id: str
    question_ids: list[int] = Field(default_factory=list)
    answers: dict[str, int | None] = Field(default_factory=dict)
    time_spent_sec: dict[str, int] = Field(default_factory=dict)


class ManualAttemptRequest(BaseModel):
    user_name: str = "default"
    mode: str = "bank-practice"
    question_id: int
    selected_option: int | None = None
    is_correct: bool | None = None
    time_spent_sec: int = Field(default=0, ge=0, le=7200)


class RevisionGenerateRequest(BaseModel):
    user_name: str = "default"
    days: int = Field(default=60, ge=1, le=365)
    daily_question_target: int = Field(default=60, ge=10, le=200)
    weak_topics: list[str] = Field(default_factory=list)


class RevisionMarkRequest(BaseModel):
    user_name: str = "default"
    plan_date: str
    completed: bool = True


class FlashcardsGenerateRequest(BaseModel):
    user_name: str = "default"
    limit: int = Field(default=200, ge=1, le=1000)


class FlashcardReviewRequest(BaseModel):
    user_name: str = "default"
    question_id: int
    rating: str = "good"


class AIAskRequest(BaseModel):
    user_name: str = "default"
    prompt: str
    context: str = ""


class AIExplainRequest(BaseModel):
    user_name: str = "default"
    question_id: int
    selected_option: int | None = None


class GoalSetRequest(BaseModel):
    user_name: str = "default"
    target_score: int = Field(default=650, ge=100, le=720)
    exam_date: str
    daily_question_goal: int = Field(default=60, ge=10, le=300)


class QOTDSubmitRequest(BaseModel):
    user_name: str = "default"
    question_id: int
    selected_option: int | None = None
    time_spent_sec: int = Field(default=60, ge=0, le=7200)


class MockPaperSectionRequest(BaseModel):
    name: str = "Section"
    subject: str | None = None
    topic: str | None = None
    question_type: str | None = None
    count: int = Field(default=30, ge=1, le=300)


class MockPaperBuildRequest(BaseModel):
    user_name: str = "default"
    title: str = "Custom Mock Paper"
    total_questions: int = Field(default=180, ge=5, le=300)
    duration_minutes: int = Field(default=180, ge=10, le=360)
    subjects: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    question_types: list[str] = Field(default_factory=list)
    source_years: list[int] = Field(default_factory=list)
    difficulties: list[str] = Field(default_factory=list)
    only_tagged: bool = True
    only_pyq: bool = False
    sections: list[MockPaperSectionRequest] = Field(default_factory=list)


def _resolve_origins() -> list[str]:
    raw = str(os.getenv("CORS_ORIGINS", "")).strip()
    if not raw:
        return DEFAULT_ORIGINS
    parts = [item.strip() for item in raw.split(",")]
    return [item for item in parts if item]


def _connect_db() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    with conn:
        conn.executescript(AUX_SCHEMA_SQL)
        _ensure_aux_migrations(conn)
    return conn


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_aux_migrations(conn: sqlite3.Connection) -> None:
    flashcard_columns = _table_columns(conn, "ui_flashcards")
    migration_sql: list[str] = []

    if "lapse_count" not in flashcard_columns:
        migration_sql.append("ALTER TABLE ui_flashcards ADD COLUMN lapse_count INTEGER DEFAULT 0")
    if "review_count" not in flashcard_columns:
        migration_sql.append("ALTER TABLE ui_flashcards ADD COLUMN review_count INTEGER DEFAULT 0")
    if "is_leech" not in flashcard_columns:
        migration_sql.append("ALTER TABLE ui_flashcards ADD COLUMN is_leech INTEGER DEFAULT 0")

    for statement in migration_sql:
        conn.execute(statement)


def _parse_options(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]


def _row_to_question(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["options"] = _parse_options(item.get("options"))
    return item


def _normalize_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _normalize_subject_name(value: Any) -> str:
    key = str(value or "").strip().lower()
    return _ALLOWED_SUBJECT_LOOKUP.get(key, "")


def _filter_allowed_subjects(values: list[str] | None) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        normalized = _normalize_subject_name(raw)
        if normalized and normalized not in seen:
            seen.add(normalized)
            output.append(normalized)
    return output


def _allowed_subject_params() -> list[str]:
    return list(_ALLOWED_SUBJECTS_LOWER)


def _subject_expr(column: str = "subject") -> str:
    return f"COALESCE(NULLIF(TRIM({column}), ''), 'Unknown')"


def _allowed_subject_where(column: str = "subject") -> str:
    placeholders = ",".join(["?" for _ in _ALLOWED_SUBJECTS_LOWER])
    return f"LOWER({_subject_expr(column)}) IN ({placeholders})"


def _coerce_option(value: Any) -> int | None:
    if value is None:
        return None

    text = str(value).strip().upper()
    if not text:
        return None

    digit_match = re.search(r"\b([1-4])\b", text)
    if digit_match:
        return int(digit_match.group(1))

    letter_match = re.search(r"\b([ABCD])\b", text)
    if letter_match:
        return {"A": 1, "B": 2, "C": 3, "D": 4}[letter_match.group(1)]

    return None


def _latest_answer_option(conn: sqlite3.Connection, question_id: int) -> int | None:
    row = conn.execute(
        "SELECT answer FROM answer_keys WHERE question_id = ? ORDER BY id DESC LIMIT 1",
        (int(question_id),),
    ).fetchone()
    if row is None:
        return None
    return _coerce_option(row[0])


def _latest_answer_map(conn: sqlite3.Connection, question_ids: list[int]) -> dict[int, int | None]:
    result: dict[int, int | None] = {}
    for qid in question_ids:
        result[int(qid)] = _latest_answer_option(conn, int(qid))
    return result


def _classify_mistake_type(question_text: str, selected_option: int | None, correct_option: int | None, time_spent_sec: int) -> str:
    if selected_option is None:
        return "Skipped"

    if correct_option is None:
        return "Ungraded"

    if int(selected_option) == int(correct_option):
        return "No Mistake"

    text_l = str(question_text or "").lower()
    conceptual_markers = ["assertion", "reason", "statement", "which of the following", "match"]
    formula_markers = ["calculate", "find", "numerical", "velocity", "current", "mole", "enthalpy"]

    if int(time_spent_sec or 0) < 25:
        return "Silly Mistake"
    if any(marker in text_l for marker in formula_markers):
        return "Formula Gap"
    if any(marker in text_l for marker in conceptual_markers):
        return "Concept Gap"
    if int(time_spent_sec or 0) > 130:
        return "Time Pressure Mistake"
    return "Concept Gap"


def _filtered_questions(
    conn: sqlite3.Connection,
    subjects: list[str] | None = None,
    topics: list[str] | None = None,
    question_types: list[str] | None = None,
    source_years: list[int] | None = None,
    difficulties: list[str] | None = None,
    search_text: str = "",
    only_tagged: bool = False,
    only_pyq: bool = False,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    clean_subjects = [item.lower() for item in _filter_allowed_subjects(subjects)]
    clean_topics = [item.strip() for item in (topics or []) if str(item).strip()]
    clean_question_types = [str(item).strip().lower() for item in (question_types or []) if str(item).strip()]
    clean_years = [int(item) for item in (source_years or []) if int(item) > 0]
    clean_difficulties = [str(item).strip().lower() for item in (difficulties or []) if str(item).strip()]

    clauses.append(_allowed_subject_where("subject"))
    params.extend(_allowed_subject_params())

    if clean_subjects:
        placeholders = ",".join(["?" for _ in clean_subjects])
        clauses.append(f"LOWER({_subject_expr('subject')}) IN ({placeholders})")
        params.extend(clean_subjects)

    if clean_topics:
        placeholders = ",".join(["?" for _ in clean_topics])
        clauses.append(f"COALESCE(NULLIF(TRIM(topic), ''), 'unknown') IN ({placeholders})")
        params.extend(clean_topics)

    if clean_question_types:
        placeholders = ",".join(["?" for _ in clean_question_types])
        clauses.append(f"LOWER(COALESCE(NULLIF(TRIM(question_type), ''), 'unknown')) IN ({placeholders})")
        params.extend(clean_question_types)

    if clean_years:
        placeholders = ",".join(["?" for _ in clean_years])
        clauses.append(f"source_year IN ({placeholders})")
        params.extend(clean_years)

    if clean_difficulties:
        placeholders = ",".join(["?" for _ in clean_difficulties])
        clauses.append(f"LOWER(COALESCE(NULLIF(TRIM(difficulty), ''), 'unknown')) IN ({placeholders})")
        params.extend(clean_difficulties)

    if str(search_text).strip():
        clauses.append("question_text LIKE ?")
        params.append(f"%{str(search_text).strip()}%")

    if only_tagged:
        clauses.append("tag_confidence > 0.0")

    if only_pyq:
        clauses.append("source_year IS NOT NULL AND source_year > 0")

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(f"SELECT * FROM questions {where_sql}", params).fetchall()
    return [_row_to_question(row) for row in rows]


def _graded_accuracy_for_user(conn: sqlite3.Connection, user_name: str) -> float:
    row = conn.execute(
        """
        SELECT AVG(CASE WHEN is_correct IN (0, 1) THEN is_correct END)
        FROM ui_attempts
        WHERE user_name = ?
        """,
        (str(user_name),),
    ).fetchone()

    value = float(row[0] or 0.0) if row else 0.0
    return max(0.0, min(100.0, value * 100.0))


def _weak_topics_for_user(conn: sqlite3.Connection, user_name: str, limit: int = 8) -> list[str]:
    rows = conn.execute(
        """
        SELECT
            COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown') AS topic,
            COUNT(*) AS attempts,
            AVG(a.is_correct) AS accuracy
        FROM ui_attempts a
        JOIN questions q ON q.id = a.question_id
        WHERE a.user_name = ? AND a.is_correct IN (0, 1)
        GROUP BY COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown')
        ORDER BY accuracy ASC, attempts DESC
        LIMIT ?
        """,
        (str(user_name), max(1, int(limit))),
    ).fetchall()
    return [str(row["topic"]) for row in rows]


def _build_daily_pool(all_questions: list[dict[str, Any]], weak_topics: list[str], total_questions: int) -> list[dict[str, Any]]:
    if not all_questions:
        return []

    weak_pool = [item for item in all_questions if str(item.get("topic", "")).strip() in weak_topics]
    other_pool = [item for item in all_questions if str(item.get("topic", "")).strip() not in weak_topics]

    random.shuffle(weak_pool)
    random.shuffle(other_pool)

    picks: list[dict[str, Any]] = []
    target_weak = min(len(weak_pool), max(3, int(total_questions // 2)))
    picks.extend(weak_pool[:target_weak])

    if len(picks) < total_questions:
        picks.extend(other_pool[: total_questions - len(picks)])

    unique: dict[int, dict[str, Any]] = {}
    for item in picks:
        unique[int(item["id"])] = item

    if len(unique) < total_questions:
        remaining = [item for item in all_questions if int(item["id"]) not in unique]
        random.shuffle(remaining)
        for item in remaining:
            unique[int(item["id"])] = item
            if len(unique) >= total_questions:
                break

    result = list(unique.values())
    random.shuffle(result)
    return result[:total_questions]


def _build_adaptive_pool(conn: sqlite3.Connection, user_name: str, all_questions: list[dict[str, Any]], total_questions: int) -> list[dict[str, Any]]:
    if not all_questions:
        return []

    weak_topics = _weak_topics_for_user(conn, user_name=user_name, limit=8)
    weak_pool = [item for item in all_questions if str(item.get("topic", "")).strip() in weak_topics]
    strong_pool = [item for item in all_questions if str(item.get("topic", "")).strip() not in weak_topics]

    random.shuffle(weak_pool)
    random.shuffle(strong_pool)

    target_weak = min(len(weak_pool), max(5, int(total_questions * 0.55)))
    picks = weak_pool[:target_weak]
    if len(picks) < total_questions:
        picks.extend(strong_pool[: total_questions - len(picks)])

    accuracy = _graded_accuracy_for_user(conn, user_name=user_name)
    if accuracy >= 80:
        desired_mix = {"hard": 0.4, "medium": 0.4, "easy": 0.2}
    elif accuracy >= 65:
        desired_mix = {"hard": 0.25, "medium": 0.5, "easy": 0.25}
    else:
        desired_mix = {"hard": 0.15, "medium": 0.45, "easy": 0.4}

    by_difficulty: dict[str, list[dict[str, Any]]] = {"easy": [], "medium": [], "hard": []}
    for item in picks:
        level = str(item.get("difficulty", "medium")).strip().lower()
        if level not in by_difficulty:
            level = "medium"
        by_difficulty[level].append(item)

    adaptive: list[dict[str, Any]] = []
    for level, frac in desired_mix.items():
        target_count = int(round(total_questions * frac))
        level_pool = by_difficulty.get(level, [])
        random.shuffle(level_pool)
        adaptive.extend(level_pool[:target_count])

    if len(adaptive) < total_questions:
        existing = {int(item["id"]) for item in adaptive}
        remaining = [item for item in picks if int(item["id"]) not in existing]
        random.shuffle(remaining)
        adaptive.extend(remaining[: total_questions - len(adaptive)])

    unique_map = {int(item["id"]): item for item in adaptive}
    result = list(unique_map.values())
    random.shuffle(result)
    return result[:total_questions]


def _resolve_groq_api_key() -> str | None:
    primary = str(os.getenv("GROQ_API_KEY", "")).strip()
    if primary:
        return primary

    multi = str(os.getenv("GROQ_API_KEYS", "")).strip()
    if not multi:
        return None

    keys = [item.strip() for item in re.split(r"[\n,;]+", multi) if item.strip()]
    return keys[0] if keys else None


def _run_groq_chat(messages: list[dict[str, str]], model: str = DEFAULT_AI_MODEL, temperature: float = 0.2, max_tokens: int = 1000) -> str:
    if Groq is None:
        raise RuntimeError("Groq SDK is not installed")

    key = _resolve_groq_api_key()
    if not key:
        raise RuntimeError("GROQ_API_KEY or GROQ_API_KEYS is required")

    client = Groq(api_key=key)
    response = client.chat.completions.create(
        model=model,
        temperature=float(temperature),
        max_tokens=int(max_tokens),
        messages=messages,
    )
    return str((response.choices[0].message.content if response.choices else "") or "").strip()


def _today_iso() -> str:
    return date.today().isoformat()


def _daily_streak_from_conn(conn: sqlite3.Connection, user_name: str) -> int:
    rows = conn.execute(
        """
        SELECT DATE(submitted_at) AS day
        FROM ui_sessions
        WHERE user_name = ? AND mode = 'daily-quiz'
        GROUP BY DATE(submitted_at)
        ORDER BY day DESC
        """,
        (str(user_name),),
    ).fetchall()

    date_texts = {str(row["day"]) for row in rows if row["day"]}
    streak = 0
    cursor = date.today()
    while cursor.isoformat() in date_texts:
        streak += 1
        cursor -= timedelta(days=1)
    return streak


def _compute_forecast(scores: list[float], current_accuracy: float) -> dict[str, Any]:
    if not scores:
        return {
            "predicted_score": 0,
            "low": 0,
            "high": 0,
            "confidence": "Low",
            "current_accuracy": round(float(current_accuracy), 2),
            "recommended_accuracy": 75.0,
        }

    recent = scores[-12:]
    expected = float(mean(recent))
    spread = float(pstdev(recent)) if len(recent) > 1 else 20.0
    low = max(0, round(expected - spread))
    high = min(720, round(expected + spread))

    if len(recent) >= 8:
        confidence = "High"
    elif len(recent) >= 4:
        confidence = "Medium"
    else:
        confidence = "Low"

    recommended_accuracy = max(55.0, min(95.0, 65.0 + (500.0 - expected) / 8.0))
    return {
        "predicted_score": round(expected),
        "low": low,
        "high": high,
        "confidence": confidence,
        "current_accuracy": round(float(current_accuracy), 2),
        "recommended_accuracy": round(float(recommended_accuracy), 2),
    }


def _parse_iso_date(value: str) -> date:
    text = str(value or "").strip()
    try:
        return datetime.fromisoformat(text).date()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid ISO date: {value}") from exc


def _score_to_rank_projection(score: float) -> dict[str, Any]:
    anchors = [
        (0.0, 900000.0, 0.10),
        (100.0, 820000.0, 1.20),
        (200.0, 670000.0, 6.00),
        (300.0, 450000.0, 22.00),
        (400.0, 260000.0, 50.00),
        (500.0, 100000.0, 80.00),
        (600.0, 20000.0, 96.00),
        (650.0, 5000.0, 98.80),
        (680.0, 1000.0, 99.70),
        (700.0, 100.0, 99.97),
        (720.0, 1.0, 99.999),
    ]

    value = max(0.0, min(720.0, float(score)))
    for idx in range(len(anchors) - 1):
        x1, rank1, pct1 = anchors[idx]
        x2, rank2, pct2 = anchors[idx + 1]
        if x1 <= value <= x2:
            span = max(1e-9, (x2 - x1))
            ratio = (value - x1) / span
            rank = rank1 + (rank2 - rank1) * ratio
            pct = pct1 + (pct2 - pct1) * ratio
            return {
                "score": round(value, 2),
                "estimated_rank": int(round(rank)),
                "estimated_percentile": round(float(pct), 3),
            }

    x, rank, pct = anchors[-1]
    return {
        "score": round(float(x), 2),
        "estimated_rank": int(round(rank)),
        "estimated_percentile": round(float(pct), 3),
    }


def _topic_accuracy_for_question(conn: sqlite3.Connection, user_name: str, question_id: int) -> float:
    row = conn.execute(
        """
        SELECT AVG(a.is_correct) AS accuracy
        FROM ui_attempts a
        JOIN questions qa ON qa.id = a.question_id
        JOIN questions qref ON qref.id = ?
        WHERE a.user_name = ?
          AND a.is_correct IN (0, 1)
          AND COALESCE(NULLIF(TRIM(qa.topic), ''), 'unknown') = COALESCE(NULLIF(TRIM(qref.topic), ''), 'unknown')
        """,
        (int(question_id), str(user_name)),
    ).fetchone()

    return round(float((row["accuracy"] if row else 0.0) or 0.0) * 100.0, 2)


def _mastery_score(accuracy: float, avg_time_sec: float, attempts: int) -> float:
    speed_score = max(0.0, min(100.0, 110.0 - (float(avg_time_sec) * 0.6)))
    volume_score = max(0.0, min(100.0, float(attempts) * 8.0))
    score = (0.65 * float(accuracy)) + (0.2 * speed_score) + (0.15 * volume_score)
    return round(max(0.0, min(100.0, score)), 2)


def _qotd_question_id(conn: sqlite3.Connection, user_name: str, qotd_date: str | None = None) -> int:
    date_text = str(qotd_date or _today_iso())

    tagged_ids = [
        int(row[0])
        for row in conn.execute(
            "SELECT id FROM questions WHERE tag_confidence > 0.0 ORDER BY id ASC"
        ).fetchall()
    ]

    ids = tagged_ids or [
        int(row[0])
        for row in conn.execute("SELECT id FROM questions ORDER BY id ASC").fetchall()
    ]

    if not ids:
        raise HTTPException(status_code=404, detail="No questions available for QOTD")

    token = hashlib.md5(f"{user_name}|{date_text}".encode("utf-8")).hexdigest()
    idx = int(token[:12], 16) % len(ids)
    return int(ids[idx])


app = FastAPI(title=APP_TITLE, version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_resolve_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, Any]:
    db_exists = DB_PATH.exists()
    return {
        "status": "ok",
        "database": str(DB_PATH),
        "database_exists": db_exists,
    }


@app.get("/api/meta/options")
def get_options() -> dict[str, Any]:
    conn = _connect_db()
    try:
        subjects = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT COALESCE(NULLIF(TRIM(subject), ''), 'Unknown') FROM questions ORDER BY 1"
            ).fetchall()
        ]
        topics = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT COALESCE(NULLIF(TRIM(topic), ''), 'unknown') FROM questions ORDER BY 1"
            ).fetchall()
        ]
        years = [
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT source_year FROM questions WHERE source_year IS NOT NULL AND source_year > 0 ORDER BY source_year DESC"
            ).fetchall()
        ]
        difficulties = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT LOWER(COALESCE(NULLIF(TRIM(difficulty), ''), 'unknown')) FROM questions ORDER BY 1"
            ).fetchall()
        ]
        question_types = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT LOWER(COALESCE(NULLIF(TRIM(question_type), ''), 'unknown')) FROM questions ORDER BY 1"
            ).fetchall()
        ]

        return {
            "subjects": subjects,
            "topics": topics,
            "source_years": years,
            "difficulties": difficulties,
            "question_types": question_types,
            "modes": [
                "exam",
                "adaptive",
                "omr",
                "pyq",
                "daily-quiz",
                "bank-practice",
            ],
        }
    finally:
        conn.close()


@app.get("/api/users")
def get_users() -> dict[str, Any]:
    conn = _connect_db()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT user_name FROM (
                SELECT user_name FROM ui_attempts
                UNION ALL
                SELECT user_name FROM ui_sessions
                UNION ALL
                SELECT user_name FROM ui_revision_plan
                UNION ALL
                SELECT user_name FROM ui_flashcards
            )
            ORDER BY user_name
            """
        ).fetchall()
        users = [str(row[0]) for row in rows if str(row[0]).strip()]
        if "default" not in users:
            users.insert(0, "default")
        return {"users": users}
    finally:
        conn.close()


@app.get("/api/overview")
def get_overview() -> dict[str, Any]:
    conn = _connect_db()
    try:
        total = int(conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0])
        tagged = int(conn.execute("SELECT COUNT(*) FROM questions WHERE tag_confidence > 0.0").fetchone()[0])
        pending = max(0, total - tagged)
        avg_conf = float(
            conn.execute(
                "SELECT COALESCE(AVG(tag_confidence), 0.0) FROM questions WHERE tag_confidence > 0.0"
            ).fetchone()[0]
            or 0.0
        )

        by_subject_rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(subject), ''), 'Unknown') AS subject,
                COUNT(*) AS total,
                SUM(CASE WHEN tag_confidence > 0.0 THEN 1 ELSE 0 END) AS tagged
            FROM questions
            GROUP BY COALESCE(NULLIF(TRIM(subject), ''), 'Unknown')
            ORDER BY total DESC
            """
        ).fetchall()

        by_difficulty_rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(difficulty), ''), 'unknown') AS difficulty,
                COUNT(*) AS total
            FROM questions
            GROUP BY COALESCE(NULLIF(TRIM(difficulty), ''), 'unknown')
            ORDER BY total DESC
            """
        ).fetchall()

        by_year_rows = conn.execute(
            """
            SELECT source_year, COUNT(*) AS total
            FROM questions
            WHERE source_year IS NOT NULL AND source_year > 0
            GROUP BY source_year
            ORDER BY source_year DESC
            LIMIT 15
            """
        ).fetchall()

        return {
            "total_questions": total,
            "tagged_questions": tagged,
            "pending_questions": pending,
            "tagged_pct": round((tagged / total) * 100, 2) if total else 0.0,
            "average_tag_confidence": round(avg_conf, 4),
            "by_subject": [
                {
                    "subject": str(row["subject"]),
                    "total": int(row["total"] or 0),
                    "tagged": int(row["tagged"] or 0),
                }
                for row in by_subject_rows
            ],
            "by_difficulty": [
                {
                    "difficulty": str(row["difficulty"]),
                    "total": int(row["total"] or 0),
                }
                for row in by_difficulty_rows
            ],
            "recent_year_distribution": [
                {
                    "source_year": int(row["source_year"] or 0),
                    "total": int(row["total"] or 0),
                }
                for row in by_year_rows
            ],
        }
    finally:
        conn.close()


@app.get("/api/tagging-progress")
def get_tagging_progress() -> dict[str, Any]:
    conn = _connect_db()
    try:
        total = int(conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0])
        tagged = int(conn.execute("SELECT COUNT(*) FROM questions WHERE tag_confidence > 0.0").fetchone()[0])
        pending = max(0, total - tagged)

        buckets = conn.execute(
            """
            SELECT
                CASE
                    WHEN tag_confidence >= 0.9 THEN '0.90-1.00'
                    WHEN tag_confidence >= 0.7 THEN '0.70-0.89'
                    WHEN tag_confidence >= 0.5 THEN '0.50-0.69'
                    WHEN tag_confidence > 0.0 THEN '0.01-0.49'
                    ELSE '0.00'
                END AS band,
                COUNT(*) AS total
            FROM questions
            GROUP BY band
            ORDER BY band DESC
            """
        ).fetchall()

        return {
            "total": total,
            "tagged": tagged,
            "pending": pending,
            "progress_pct": round((tagged / total) * 100, 2) if total else 0.0,
            "confidence_bands": [
                {
                    "band": str(row["band"]),
                    "total": int(row["total"] or 0),
                }
                for row in buckets
            ],
        }
    finally:
        conn.close()


@app.get("/api/data/summary")
def get_data_summary() -> dict[str, Any]:
    conn = _connect_db()
    try:
        total_questions = int(conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0])
        keyed_questions = int(
            conn.execute("SELECT COUNT(DISTINCT question_id) FROM answer_keys").fetchone()[0]
        )
        attempts = int(conn.execute("SELECT COUNT(*) FROM ui_attempts").fetchone()[0])
        sessions = int(conn.execute("SELECT COUNT(*) FROM ui_sessions").fetchone()[0])

        source_pdf_count = int(
            conn.execute(
                "SELECT COUNT(DISTINCT source_pdf) FROM questions WHERE source_pdf IS NOT NULL AND TRIM(source_pdf) != ''"
            ).fetchone()[0]
        )

        return {
            "database": str(DB_PATH),
            "total_questions": total_questions,
            "answer_key_coverage": keyed_questions,
            "attempt_logs": attempts,
            "session_reports": sessions,
            "distinct_source_pdfs": source_pdf_count,
            "files": {
                "questions_json": str(Path("data/questions.json").exists()),
                "study_state_json": str(Path("data/study_state.json").exists()),
                "manifest_json": str(Path("data/manifest.json").exists()),
            },
        }
    finally:
        conn.close()


@app.get("/api/topics")
def get_topics(
    subject: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    conn = _connect_db()
    try:
        params: list[Any] = []
        where = ""
        if subject:
            where = "WHERE COALESCE(NULLIF(TRIM(subject), ''), 'Unknown') = ?"
            params.append(subject.strip())

        rows = conn.execute(
            f"""
            SELECT
                COALESCE(NULLIF(TRIM(topic), ''), 'unknown') AS topic,
                COUNT(*) AS total,
                SUM(CASE WHEN tag_confidence > 0.0 THEN 1 ELSE 0 END) AS tagged
            FROM questions
            {where}
            GROUP BY COALESCE(NULLIF(TRIM(topic), ''), 'unknown')
            ORDER BY total DESC
            LIMIT ?
            """,
            [*params, int(limit)],
        ).fetchall()

        return {
            "items": [
                {
                    "topic": str(row["topic"]),
                    "total": int(row["total"] or 0),
                    "tagged": int(row["tagged"] or 0),
                }
                for row in rows
            ]
        }
    finally:
        conn.close()


@app.get("/api/questions")
def get_questions(
    q: str | None = Query(default=None),
    subject: str | None = Query(default=None),
    topic: str | None = Query(default=None),
    question_type: str | None = Query(default=None),
    difficulty: str | None = Query(default=None),
    source_year: int | None = Query(default=None),
    only_tagged: bool = Query(default=False),
    limit: int = Query(default=20, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    conn = _connect_db()
    try:
        clauses: list[str] = []
        params: list[Any] = []

        if q:
            clauses.append("question_text LIKE ?")
            params.append(f"%{q.strip()}%")

        if subject:
            clauses.append("COALESCE(NULLIF(TRIM(subject), ''), 'Unknown') = ?")
            params.append(subject.strip())

        if topic:
            clauses.append("COALESCE(NULLIF(TRIM(topic), ''), 'unknown') = ?")
            params.append(topic.strip())

        if question_type:
            clauses.append("LOWER(COALESCE(NULLIF(TRIM(question_type), ''), 'unknown')) = ?")
            params.append(question_type.strip().lower())

        if difficulty:
            clauses.append("LOWER(COALESCE(NULLIF(TRIM(difficulty), ''), 'unknown')) = ?")
            params.append(difficulty.strip().lower())

        if source_year is not None and int(source_year) > 0:
            clauses.append("source_year = ?")
            params.append(int(source_year))

        if only_tagged:
            clauses.append("tag_confidence > 0.0")

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        total = int(conn.execute(f"SELECT COUNT(*) FROM questions {where_sql}", params).fetchone()[0])
        rows = conn.execute(
            f"""
            SELECT *
            FROM questions
            {where_sql}
            ORDER BY id ASC
            LIMIT ? OFFSET ?
            """,
            [*params, int(limit), int(offset)],
        ).fetchall()

        return {
            "total": total,
            "limit": int(limit),
            "offset": int(offset),
            "items": [_row_to_question(row) for row in rows],
        }
    finally:
        conn.close()


@app.get("/api/questions/{question_id}")
def get_question(question_id: int) -> dict[str, Any]:
    conn = _connect_db()
    try:
        row = conn.execute("SELECT * FROM questions WHERE id = ?", (int(question_id),)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Question not found")

        answer_row = conn.execute(
            """
            SELECT answer, explanation, source
            FROM answer_keys
            WHERE question_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(question_id),),
        ).fetchone()

        question = _row_to_question(row)
        latest_answer = dict(answer_row) if answer_row is not None else None

        return {
            "question": question,
            "latest_answer": latest_answer,
        }
    finally:
        conn.close()


@app.post("/api/practice/start")
def start_practice(request: PracticeStartRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        mode = str(request.mode or "exam").strip().lower()
        only_pyq = bool(request.only_pyq or mode == "pyq")

        pool = _filtered_questions(
            conn,
            subjects=request.subjects,
            topics=request.topics,
            question_types=request.question_types,
            source_years=request.source_years,
            difficulties=request.difficulties,
            search_text=request.search_text,
            only_tagged=bool(request.only_tagged),
            only_pyq=only_pyq,
        )

        if not pool:
            raise HTTPException(status_code=404, detail="No questions match current filters")

        count = min(max(1, int(request.count)), len(pool))

        if mode == "daily-quiz":
            weak_topics = _weak_topics_for_user(conn, user_name=request.user_name, limit=5)
            selected = _build_daily_pool(pool, weak_topics=weak_topics, total_questions=count)
        elif mode == "adaptive":
            selected = _build_adaptive_pool(conn, user_name=request.user_name, all_questions=pool, total_questions=count)
        else:
            selected = random.sample(pool, k=count) if len(pool) > count else pool

        random.shuffle(selected)
        session_id = f"{mode}-{uuid4().hex[:12]}"
        started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

        return {
            "session_id": session_id,
            "user_name": request.user_name,
            "mode": mode,
            "duration_minutes": int(request.duration_minutes),
            "started_at": started_at,
            "question_ids": [int(item["id"]) for item in selected],
            "questions": selected,
            "pool_size": len(pool),
        }
    finally:
        conn.close()


@app.post("/api/practice/submit")
def submit_practice(request: PracticeSubmitRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        mode = str(request.mode or "exam").strip().lower()
        question_ids = [int(item) for item in request.question_ids if int(item) > 0]
        if not question_ids:
            for raw_qid in request.answers.keys():
                try:
                    question_ids.append(int(raw_qid))
                except Exception:
                    continue

        if not question_ids:
            raise HTTPException(status_code=400, detail="No question ids provided")

        unique_qids = sorted({int(item) for item in question_ids})
        placeholders = ",".join(["?" for _ in unique_qids])
        rows = conn.execute(
            f"SELECT * FROM questions WHERE id IN ({placeholders}) ORDER BY id",
            unique_qids,
        ).fetchall()
        questions = [_row_to_question(row) for row in rows]
        question_lookup = {int(item["id"]): item for item in questions}

        if not question_lookup:
            raise HTTPException(status_code=404, detail="Questions not found for submission")

        answer_map = _latest_answer_map(conn, list(question_lookup.keys()))

        attempted = 0
        graded = 0
        correct = 0
        wrong = 0
        score = 0
        total_time_spent = 0
        details: list[dict[str, Any]] = []

        with conn:
            for qid in unique_qids:
                question = question_lookup.get(int(qid))
                if not question:
                    continue

                selected = _coerce_option(request.answers.get(str(qid)))
                if selected is None:
                    selected = _coerce_option(request.answers.get(str(int(qid))))

                time_spent = int(request.time_spent_sec.get(str(qid), 0) or 0)
                time_spent = max(0, min(7200, time_spent))
                total_time_spent += time_spent

                if selected is not None:
                    attempted += 1

                correct_option = answer_map.get(int(qid))
                is_correct: bool | None = None
                if selected is not None and correct_option is not None:
                    is_correct = int(selected) == int(correct_option)
                    graded += 1
                    if is_correct:
                        correct += 1
                        score += 4
                    else:
                        wrong += 1
                        score -= 1

                mistake_type = _classify_mistake_type(
                    question_text=str(question.get("question_text", "")),
                    selected_option=selected,
                    correct_option=correct_option,
                    time_spent_sec=time_spent,
                )

                if selected is not None:
                    conn.execute(
                        """
                        INSERT INTO ui_attempts(
                            user_name, mode, session_id, question_id, selected_option,
                            correct_option, is_correct, time_spent_sec, mistake_type
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            request.user_name,
                            mode,
                            request.session_id,
                            int(qid),
                            int(selected),
                            correct_option,
                            None if is_correct is None else (1 if is_correct else 0),
                            int(time_spent),
                            mistake_type,
                        ),
                    )

                details.append(
                    {
                        "question_id": int(qid),
                        "selected": selected,
                        "answer": correct_option,
                        "correct": is_correct,
                        "subject": question.get("subject", "Unknown"),
                        "topic": question.get("topic", "unknown"),
                        "difficulty": question.get("difficulty", "unknown"),
                        "time_spent_sec": int(time_spent),
                        "mistake_type": mistake_type,
                    }
                )

            accuracy = round((correct / graded) * 100.0, 2) if graded else 0.0
            report = {
                "submitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "session_id": request.session_id,
                "mode": mode,
                "total_questions": len(unique_qids),
                "attempted": attempted,
                "graded": graded,
                "correct": correct,
                "wrong": wrong,
                "score": score,
                "accuracy": accuracy,
                "total_time_spent_sec": int(total_time_spent),
                "avg_time_per_question_sec": round(total_time_spent / max(1, len(unique_qids)), 2),
                "details": details,
            }

            conn.execute(
                """
                INSERT INTO ui_sessions(
                    user_name, session_id, mode, total_questions, attempted,
                    graded, correct, wrong, score, accuracy, avg_time_per_question_sec
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_name, session_id, mode)
                DO UPDATE SET
                    total_questions = excluded.total_questions,
                    attempted = excluded.attempted,
                    graded = excluded.graded,
                    correct = excluded.correct,
                    wrong = excluded.wrong,
                    score = excluded.score,
                    accuracy = excluded.accuracy,
                    avg_time_per_question_sec = excluded.avg_time_per_question_sec,
                    submitted_at = datetime('now')
                """,
                (
                    request.user_name,
                    request.session_id,
                    mode,
                    int(report["total_questions"]),
                    int(report["attempted"]),
                    int(report["graded"]),
                    int(report["correct"]),
                    int(report["wrong"]),
                    int(report["score"]),
                    float(report["accuracy"]),
                    float(report["avg_time_per_question_sec"]),
                ),
            )

        return report
    finally:
        conn.close()


@app.post("/api/mock-paper/build")
def build_mock_paper(request: MockPaperBuildRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        pool = _filtered_questions(
            conn,
            subjects=request.subjects,
            topics=request.topics,
            question_types=request.question_types,
            source_years=request.source_years,
            difficulties=request.difficulties,
            search_text="",
            only_tagged=bool(request.only_tagged),
            only_pyq=bool(request.only_pyq),
        )

        if not pool:
            raise HTTPException(status_code=404, detail="No questions available for mock paper with current filters")

        target_total = min(max(1, int(request.total_questions)), len(pool))
        selected: list[dict[str, Any]] = []
        available: dict[int, dict[str, Any]] = {int(item["id"]): item for item in pool}
        section_summary: list[dict[str, Any]] = []

        for section in request.sections:
            remaining_slots = target_total - len(selected)
            if remaining_slots <= 0:
                break

            section_count = min(int(section.count), remaining_slots)
            section_candidates = [
                item
                for item in available.values()
                if (
                    (not section.subject or str(item.get("subject") or "").strip().lower() == str(section.subject).strip().lower())
                    and (not section.topic or str(item.get("topic") or "").strip().lower() == str(section.topic).strip().lower())
                    and (
                        not section.question_type
                        or str(item.get("question_type") or "").strip().lower() == str(section.question_type).strip().lower()
                    )
                )
            ]

            random.shuffle(section_candidates)
            picked = section_candidates[:section_count]
            for item in picked:
                qid = int(item["id"])
                if qid in available:
                    selected.append(item)
                    del available[qid]

            section_summary.append(
                {
                    "name": str(section.name or "Section"),
                    "requested": int(section.count),
                    "selected": len(picked),
                    "subject": section.subject,
                    "topic": section.topic,
                    "question_type": section.question_type,
                }
            )

        if len(selected) < target_total:
            remaining_pool = list(available.values())
            random.shuffle(remaining_pool)
            selected.extend(remaining_pool[: target_total - len(selected)])

        if not selected:
            raise HTTPException(status_code=404, detail="Could not build mock paper from current configuration")

        random.shuffle(selected)
        question_ids = [int(item["id"]) for item in selected]
        config = {
            "title": str(request.title),
            "total_questions": int(request.total_questions),
            "duration_minutes": int(request.duration_minutes),
            "subjects": request.subjects,
            "topics": request.topics,
            "question_types": request.question_types,
            "source_years": request.source_years,
            "difficulties": request.difficulties,
            "only_tagged": bool(request.only_tagged),
            "only_pyq": bool(request.only_pyq),
            "sections": [section.model_dump() for section in request.sections],
        }

        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with conn:
            conn.execute(
                """
                INSERT INTO ui_mock_papers(
                    user_name, title, total_questions, duration_minutes, config_json, question_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(request.user_name),
                    str(request.title),
                    int(len(question_ids)),
                    int(request.duration_minutes),
                    json.dumps(config, ensure_ascii=False),
                    json.dumps(question_ids),
                ),
            )
            paper_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        return {
            "paper_id": paper_id,
            "user_name": request.user_name,
            "title": request.title,
            "created_at": created_at,
            "total_questions": len(question_ids),
            "duration_minutes": int(request.duration_minutes),
            "question_ids": question_ids,
            "sections": section_summary,
            "questions": selected,
        }
    finally:
        conn.close()


@app.get("/api/mock-paper/list")
def list_mock_papers(
    user_name: str = Query(default="default"),
    limit: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    conn = _connect_db()
    try:
        rows = conn.execute(
            """
            SELECT id, title, total_questions, duration_minutes, created_at
            FROM ui_mock_papers
            WHERE user_name = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (str(user_name), int(limit)),
        ).fetchall()

        return {
            "user_name": user_name,
            "items": [
                {
                    "paper_id": int(row["id"]),
                    "title": str(row["title"]),
                    "total_questions": int(row["total_questions"] or 0),
                    "duration_minutes": int(row["duration_minutes"] or 0),
                    "created_at": str(row["created_at"]),
                }
                for row in rows
            ],
        }
    finally:
        conn.close()


@app.get("/api/mock-paper/{paper_id}")
def get_mock_paper(paper_id: int) -> dict[str, Any]:
    conn = _connect_db()
    try:
        row = conn.execute(
            """
            SELECT id, user_name, title, total_questions, duration_minutes, config_json, question_ids_json, created_at
            FROM ui_mock_papers
            WHERE id = ?
            LIMIT 1
            """,
            (int(paper_id),),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Mock paper not found")

        try:
            question_ids = [int(item) for item in json.loads(str(row["question_ids_json"] or "[]"))]
        except Exception:
            question_ids = []

        try:
            config_json = json.loads(str(row["config_json"] or "{}"))
        except Exception:
            config_json = {}

        questions: list[dict[str, Any]] = []
        if question_ids:
            placeholders = ",".join(["?" for _ in question_ids])
            q_rows = conn.execute(
                f"SELECT * FROM questions WHERE id IN ({placeholders})",
                question_ids,
            ).fetchall()
            q_map = {int(q_row["id"]): _row_to_question(q_row) for q_row in q_rows}
            questions = [q_map[qid] for qid in question_ids if qid in q_map]

        return {
            "paper_id": int(row["id"]),
            "user_name": str(row["user_name"]),
            "title": str(row["title"]),
            "total_questions": int(row["total_questions"] or 0),
            "duration_minutes": int(row["duration_minutes"] or 0),
            "created_at": str(row["created_at"]),
            "config": config_json,
            "question_ids": question_ids,
            "questions": questions,
        }
    finally:
        conn.close()


@app.post("/api/attempts/log")
def log_attempt(request: ManualAttemptRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        row = conn.execute("SELECT * FROM questions WHERE id = ?", (int(request.question_id),)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Question not found")

        selected = _coerce_option(request.selected_option)
        correct_option = _latest_answer_option(conn, int(request.question_id))

        resolved_correct = request.is_correct
        if resolved_correct is None and selected is not None and correct_option is not None:
            resolved_correct = int(selected) == int(correct_option)

        question = _row_to_question(row)
        mistake_type = _classify_mistake_type(
            question_text=str(question.get("question_text", "")),
            selected_option=selected,
            correct_option=correct_option,
            time_spent_sec=int(request.time_spent_sec),
        )

        with conn:
            conn.execute(
                """
                INSERT INTO ui_attempts(
                    user_name, mode, session_id, question_id, selected_option,
                    correct_option, is_correct, time_spent_sec, mistake_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.user_name,
                    request.mode,
                    f"manual-{uuid4().hex[:10]}",
                    int(request.question_id),
                    selected,
                    correct_option,
                    None if resolved_correct is None else (1 if resolved_correct else 0),
                    int(request.time_spent_sec),
                    mistake_type,
                ),
            )

        return {
            "status": "ok",
            "question_id": int(request.question_id),
            "selected": selected,
            "correct_option": correct_option,
            "is_correct": resolved_correct,
            "mistake_type": mistake_type,
        }
    finally:
        conn.close()


@app.get("/api/analytics/time")
def analytics_time(user_name: str = Query(default="default")) -> dict[str, Any]:
    conn = _connect_db()
    try:
        subject_rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(q.subject), ''), 'Unknown') AS subject,
                ROUND(AVG(a.time_spent_sec), 2) AS avg_time_sec,
                COUNT(*) AS attempts
            FROM ui_attempts a
            JOIN questions q ON q.id = a.question_id
            WHERE a.user_name = ?
            GROUP BY COALESCE(NULLIF(TRIM(q.subject), ''), 'Unknown')
            ORDER BY avg_time_sec DESC
            """,
            (str(user_name),),
        ).fetchall()

        topic_rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown') AS topic,
                ROUND(AVG(a.time_spent_sec), 2) AS avg_time_sec,
                COUNT(*) AS attempts
            FROM ui_attempts a
            JOIN questions q ON q.id = a.question_id
            WHERE a.user_name = ?
            GROUP BY COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown')
            ORDER BY avg_time_sec DESC
            LIMIT 20
            """,
            (str(user_name),),
        ).fetchall()

        slow_rows = conn.execute(
            """
            SELECT
                a.question_id,
                ROUND(AVG(a.time_spent_sec), 2) AS avg_time_sec,
                COUNT(*) AS attempts
            FROM ui_attempts a
            WHERE a.user_name = ?
            GROUP BY a.question_id
            ORDER BY avg_time_sec DESC
            LIMIT 20
            """,
            (str(user_name),),
        ).fetchall()

        overall_row = conn.execute(
            "SELECT ROUND(AVG(time_spent_sec), 2) FROM ui_attempts WHERE user_name = ?",
            (str(user_name),),
        ).fetchone()

        return {
            "user_name": user_name,
            "average_time_sec": float((overall_row[0] if overall_row else 0.0) or 0.0),
            "by_subject": [
                {
                    "subject": str(row["subject"]),
                    "avg_time_sec": float(row["avg_time_sec"] or 0.0),
                    "attempts": int(row["attempts"] or 0),
                }
                for row in subject_rows
            ],
            "by_topic": [
                {
                    "topic": str(row["topic"]),
                    "avg_time_sec": float(row["avg_time_sec"] or 0.0),
                    "attempts": int(row["attempts"] or 0),
                }
                for row in topic_rows
            ],
            "slowest_questions": [
                {
                    "question_id": int(row["question_id"]),
                    "avg_time_sec": float(row["avg_time_sec"] or 0.0),
                    "attempts": int(row["attempts"] or 0),
                }
                for row in slow_rows
            ],
        }
    finally:
        conn.close()


@app.get("/api/analytics/weakness")
def analytics_weakness(user_name: str = Query(default="default")) -> dict[str, Any]:
    conn = _connect_db()
    try:
        rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown') AS topic,
                COUNT(*) AS attempts,
                SUM(CASE WHEN a.is_correct = 1 THEN 1 ELSE 0 END) AS correct,
                SUM(CASE WHEN a.is_correct = 0 THEN 1 ELSE 0 END) AS wrong,
                ROUND(100.0 * AVG(a.is_correct), 2) AS accuracy
            FROM ui_attempts a
            JOIN questions q ON q.id = a.question_id
            WHERE a.user_name = ? AND a.is_correct IN (0, 1)
            GROUP BY COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown')
            ORDER BY accuracy ASC, attempts DESC
            """,
            (str(user_name),),
        ).fetchall()

        top_weak = [str(row["topic"]) for row in rows[:6]]
        plan_rows: list[dict[str, Any]] = []
        if top_weak:
            for offset in range(7):
                topic = top_weak[offset % len(top_weak)]
                day = (date.today() + timedelta(days=offset)).isoformat()
                plan_rows.append(
                    {
                        "day": day,
                        "focus_topic": topic,
                        "tasks": "30 min concept review + 25 MCQs + 10 min error log",
                        "goal": "Reach at least 70% accuracy in this topic",
                    }
                )

        return {
            "user_name": user_name,
            "items": [
                {
                    "topic": str(row["topic"]),
                    "attempts": int(row["attempts"] or 0),
                    "correct": int(row["correct"] or 0),
                    "wrong": int(row["wrong"] or 0),
                    "accuracy": float(row["accuracy"] or 0.0),
                }
                for row in rows
            ],
            "recovery_plan_7d": plan_rows,
        }
    finally:
        conn.close()


@app.get("/api/mistakes/journal")
def mistakes_journal(
    user_name: str = Query(default="default"),
    limit: int = Query(default=120, ge=10, le=1000),
) -> dict[str, Any]:
    conn = _connect_db()
    try:
        rows = conn.execute(
            """
            SELECT
                a.question_id,
                a.selected_option,
                a.correct_option,
                a.time_spent_sec,
                a.mistake_type,
                a.attempted_at,
                q.question_text,
                COALESCE(NULLIF(TRIM(q.subject), ''), 'Unknown') AS subject,
                COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown') AS topic,
                COALESCE(NULLIF(TRIM(q.difficulty), ''), 'unknown') AS difficulty
            FROM ui_attempts a
            JOIN questions q ON q.id = a.question_id
            WHERE a.user_name = ? AND a.is_correct = 0
            ORDER BY a.attempted_at DESC
            LIMIT ?
            """,
            (str(user_name), int(limit)),
        ).fetchall()

        cause_rows = conn.execute(
            """
            SELECT mistake_type, COUNT(*) AS total
            FROM ui_attempts
            WHERE user_name = ? AND is_correct = 0
            GROUP BY mistake_type
            ORDER BY total DESC
            """,
            (str(user_name),),
        ).fetchall()

        weak_topics = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown') AS topic,
                COUNT(*) AS wrong_count
            FROM ui_attempts a
            JOIN questions q ON q.id = a.question_id
            WHERE a.user_name = ? AND a.is_correct = 0
            GROUP BY COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown')
            ORDER BY wrong_count DESC
            LIMIT 8
            """,
            (str(user_name),),
        ).fetchall()

        top_cause = str(cause_rows[0]["mistake_type"]) if cause_rows else "No data"
        recommendations = [
            f"Primary mistake pattern: {top_cause}. Create a 20-minute focused drill for this root cause.",
            "Review the last 10 wrong questions and write one-line error notes before attempting again.",
            "Re-attempt wrong questions in mixed mode after 48 hours to improve retention.",
        ]

        return {
            "user_name": user_name,
            "total_logged_mistakes": len(rows),
            "top_root_cause": top_cause,
            "items": [
                {
                    "question_id": int(row["question_id"]),
                    "subject": str(row["subject"]),
                    "topic": str(row["topic"]),
                    "difficulty": str(row["difficulty"]),
                    "selected_option": row["selected_option"],
                    "correct_option": row["correct_option"],
                    "time_spent_sec": int(row["time_spent_sec"] or 0),
                    "mistake_type": str(row["mistake_type"] or "Concept Gap"),
                    "attempted_at": str(row["attempted_at"]),
                    "question_text": str(row["question_text"] or ""),
                }
                for row in rows
            ],
            "root_cause_summary": [
                {"mistake_type": str(row["mistake_type"] or "Unknown"), "count": int(row["total"] or 0)}
                for row in cause_rows
            ],
            "weak_topics": [
                {"topic": str(row["topic"]), "wrong_count": int(row["wrong_count"] or 0)}
                for row in weak_topics
            ],
            "recommendations": recommendations,
        }
    finally:
        conn.close()


@app.get("/api/analytics/mastery-heatmap")
def analytics_mastery_heatmap(
    user_name: str = Query(default="default"),
    min_attempts: int = Query(default=1, ge=1, le=20),
) -> dict[str, Any]:
    conn = _connect_db()
    try:
        rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(q.subject), ''), 'Unknown') AS subject,
                COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown') AS topic,
                COUNT(*) AS attempts,
                ROUND(100.0 * AVG(a.is_correct), 2) AS accuracy,
                ROUND(AVG(a.time_spent_sec), 2) AS avg_time_sec
            FROM ui_attempts a
            JOIN questions q ON q.id = a.question_id
            WHERE a.user_name = ? AND a.is_correct IN (0, 1)
            GROUP BY COALESCE(NULLIF(TRIM(q.subject), ''), 'Unknown'), COALESCE(NULLIF(TRIM(q.topic), ''), 'unknown')
            HAVING COUNT(*) >= ?
            ORDER BY subject ASC, topic ASC
            """,
            (str(user_name), int(min_attempts)),
        ).fetchall()

        items: list[dict[str, Any]] = []
        for row in rows:
            attempts = int(row["attempts"] or 0)
            accuracy = float(row["accuracy"] or 0.0)
            avg_time = float(row["avg_time_sec"] or 0.0)
            mastery = _mastery_score(accuracy=accuracy, avg_time_sec=avg_time, attempts=attempts)
            items.append(
                {
                    "subject": str(row["subject"]),
                    "topic": str(row["topic"]),
                    "attempts": attempts,
                    "accuracy": round(accuracy, 2),
                    "avg_time_sec": round(avg_time, 2),
                    "mastery_score": mastery,
                }
            )

        weakest = sorted(items, key=lambda item: (item["mastery_score"], item["attempts"]), reverse=False)[:12]
        strongest = sorted(items, key=lambda item: (item["mastery_score"], item["attempts"]), reverse=True)[:12]

        return {
            "user_name": user_name,
            "items": items,
            "weakest_topics": weakest,
            "strongest_topics": strongest,
        }
    finally:
        conn.close()


@app.get("/api/analytics/forecast")
def analytics_forecast(user_name: str = Query(default="default")) -> dict[str, Any]:
    conn = _connect_db()
    try:
        session_rows = conn.execute(
            """
            SELECT mode, score, submitted_at, accuracy
            FROM ui_sessions
            WHERE user_name = ?
            ORDER BY submitted_at ASC
            """,
            (str(user_name),),
        ).fetchall()

        scores = [float(row["score"] or 0.0) for row in session_rows]
        current_accuracy = _graded_accuracy_for_user(conn, user_name=user_name)
        forecast = _compute_forecast(scores=scores, current_accuracy=current_accuracy)

        history = [
            {
                "mode": str(row["mode"]),
                "score": float(row["score"] or 0.0),
                "accuracy": float(row["accuracy"] or 0.0),
                "submitted_at": str(row["submitted_at"]),
            }
            for row in session_rows
        ]

        return {
            "user_name": user_name,
            "history": history,
            **forecast,
        }
    finally:
        conn.close()


@app.get("/api/analytics/rank-projection")
def analytics_rank_projection(
    user_name: str = Query(default="default"),
    score: float | None = Query(default=None),
) -> dict[str, Any]:
    conn = _connect_db()
    try:
        sessions = conn.execute(
            """
            SELECT score
            FROM ui_sessions
            WHERE user_name = ?
            ORDER BY submitted_at ASC
            """,
            (str(user_name),),
        ).fetchall()

        scores = [float(row["score"] or 0.0) for row in sessions]
        current_accuracy = _graded_accuracy_for_user(conn, user_name=user_name)
        forecast = _compute_forecast(scores=scores, current_accuracy=current_accuracy)

        predicted_score = float(score) if score is not None else float(forecast.get("predicted_score", 0.0) or 0.0)
        projection = _score_to_rank_projection(predicted_score)
        low_projection = _score_to_rank_projection(float(forecast.get("low", 0.0) or 0.0))
        high_projection = _score_to_rank_projection(float(forecast.get("high", 0.0) or 0.0))

        return {
            "user_name": user_name,
            "predicted_score": round(predicted_score, 2),
            "confidence": str(forecast.get("confidence") or "Low"),
            "projected": projection,
            "low_band": low_projection,
            "high_band": high_projection,
        }
    finally:
        conn.close()


@app.get("/api/analytics/coaching")
def analytics_coaching() -> dict[str, Any]:
    conn = _connect_db()
    try:
        user_rows = conn.execute(
            """
            SELECT DISTINCT user_name FROM (
                SELECT user_name FROM ui_attempts
                UNION ALL
                SELECT user_name FROM ui_sessions
            )
            ORDER BY user_name
            """
        ).fetchall()

        users = [str(row[0]) for row in user_rows if str(row[0]).strip()]
        if not users:
            users = ["default"]

        rows: list[dict[str, Any]] = []
        for user_name in users:
            attempts_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS attempts,
                    SUM(CASE WHEN is_correct IN (0, 1) THEN 1 ELSE 0 END) AS graded,
                    SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) AS correct,
                    SUM(CASE WHEN is_correct = 0 THEN 1 ELSE 0 END) AS wrong
                FROM ui_attempts
                WHERE user_name = ?
                """,
                (user_name,),
            ).fetchone()

            exam_count = int(
                conn.execute("SELECT COUNT(*) FROM ui_sessions WHERE user_name = ?", (user_name,)).fetchone()[0]
            )

            last_7_days_activity = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM ui_attempts
                    WHERE user_name = ? AND DATE(attempted_at) >= DATE('now', '-6 days')
                    """,
                    (user_name,),
                ).fetchone()[0]
            )

            attempts = int((attempts_row["attempts"] if attempts_row else 0) or 0)
            graded = int((attempts_row["graded"] if attempts_row else 0) or 0)
            correct = int((attempts_row["correct"] if attempts_row else 0) or 0)
            wrong = int((attempts_row["wrong"] if attempts_row else 0) or 0)
            accuracy = round((correct / graded) * 100.0, 2) if graded else 0.0

            rows.append(
                {
                    "profile": user_name,
                    "attempts": attempts,
                    "graded": graded,
                    "correct": correct,
                    "wrong": wrong,
                    "accuracy": accuracy,
                    "exams": exam_count,
                    "last_7_days_activity": last_7_days_activity,
                }
            )

        rows.sort(key=lambda item: (item["accuracy"], item["attempts"]), reverse=True)
        return {
            "items": rows,
            "recommendations": [
                "Use last_7_days_activity to identify inconsistent learners.",
                "Use weak-topic reports from each profile before assigning homework.",
                "Encourage daily-quiz streaks to improve retention.",
            ],
        }
    finally:
        conn.close()


@app.get("/api/daily/streak")
def daily_streak(user_name: str = Query(default="default")) -> dict[str, Any]:
    conn = _connect_db()
    try:
        streak = _daily_streak_from_conn(conn, user_name=user_name)

        return {
            "user_name": user_name,
            "streak": streak,
        }
    finally:
        conn.close()


@app.get("/api/daily/share-payload")
def daily_share_payload(user_name: str = Query(default="default")) -> dict[str, Any]:
    conn = _connect_db()
    try:
        streak = _daily_streak_from_conn(conn, user_name=user_name)
        last_row = conn.execute(
            """
            SELECT submitted_at, score, accuracy, correct, wrong
            FROM ui_sessions
            WHERE user_name = ? AND mode = 'daily-quiz'
            ORDER BY submitted_at DESC
            LIMIT 1
            """,
            (str(user_name),),
        ).fetchone()

        if last_row is None:
            message = (
                f"NEET Daily Quiz update for {user_name}: streak {streak} days. "
                "No quiz submitted yet today. Let us complete a 10-question drill now."
            )
            return {
                "user_name": user_name,
                "streak": streak,
                "has_report": False,
                "message": message,
            }

        score = int(last_row["score"] or 0)
        accuracy = float(last_row["accuracy"] or 0.0)
        correct = int(last_row["correct"] or 0)
        wrong = int(last_row["wrong"] or 0)
        submitted_at = str(last_row["submitted_at"])

        message = (
            f"NEET Daily Quiz update for {user_name}: score {score}, accuracy {accuracy:.1f}%, "
            f"correct {correct}, wrong {wrong}, streak {streak} days (last: {submitted_at}). "
            "Keep the streak alive with one more mixed quiz."
        )

        return {
            "user_name": user_name,
            "streak": streak,
            "has_report": True,
            "submitted_at": submitted_at,
            "score": score,
            "accuracy": round(accuracy, 2),
            "correct": correct,
            "wrong": wrong,
            "message": message,
        }
    finally:
        conn.close()


@app.get("/api/goals/current")
def get_goal_plan(user_name: str = Query(default="default")) -> dict[str, Any]:
    conn = _connect_db()
    try:
        goal_row = conn.execute(
            """
            SELECT target_score, exam_date, daily_question_goal, updated_at
            FROM ui_goals
            WHERE user_name = ?
            LIMIT 1
            """,
            (str(user_name),),
        ).fetchone()

        default_exam_date = (date.today() + timedelta(days=120)).isoformat()
        target_score = int(goal_row["target_score"] if goal_row else 650)
        exam_date_text = str(goal_row["exam_date"] if goal_row else default_exam_date)
        daily_goal = int(goal_row["daily_question_goal"] if goal_row else 60)

        exam_day = _parse_iso_date(exam_date_text)
        days_left = max(0, (exam_day - date.today()).days)
        weeks_left = max(1, (days_left + 6) // 7)

        sessions = conn.execute(
            """
            SELECT score
            FROM ui_sessions
            WHERE user_name = ?
            ORDER BY submitted_at ASC
            """,
            (str(user_name),),
        ).fetchall()
        scores = [float(row["score"] or 0.0) for row in sessions]
        current_accuracy = _graded_accuracy_for_user(conn, user_name=user_name)
        forecast = _compute_forecast(scores=scores, current_accuracy=current_accuracy)

        predicted_score = float(forecast.get("predicted_score", 0.0) or 0.0)
        target_gap = max(0.0, float(target_score) - predicted_score)
        weekly_increase = target_gap / float(max(1, weeks_left))
        required_accuracy = max(55.0, min(99.0, float(forecast.get("recommended_accuracy", 75.0)) + (target_gap / 18.0)))

        milestones: list[dict[str, Any]] = []
        for week in range(1, min(12, weeks_left) + 1):
            week_start = date.today() + timedelta(days=(week - 1) * 7)
            week_end = min(exam_day, week_start + timedelta(days=6))
            score_target = min(720.0, predicted_score + (weekly_increase * week))
            milestones.append(
                {
                    "week": week,
                    "from": week_start.isoformat(),
                    "to": week_end.isoformat(),
                    "target_score": round(score_target, 2),
                    "daily_question_goal": int(daily_goal),
                }
            )

        return {
            "user_name": user_name,
            "target_score": int(target_score),
            "exam_date": exam_date_text,
            "daily_question_goal": int(daily_goal),
            "days_left": int(days_left),
            "predicted_score": round(predicted_score, 2),
            "target_gap": round(target_gap, 2),
            "required_accuracy": round(required_accuracy, 2),
            "updated_at": str(goal_row["updated_at"]) if goal_row else None,
            "weekly_milestones": milestones,
        }
    finally:
        conn.close()


@app.get("/api/qotd")
def get_question_of_the_day(user_name: str = Query(default="default")) -> dict[str, Any]:
    conn = _connect_db()
    try:
        qotd_date = _today_iso()
        qid = _qotd_question_id(conn, user_name=user_name, qotd_date=qotd_date)

        row = conn.execute("SELECT * FROM questions WHERE id = ?", (int(qid),)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="QOTD question not found")

        answer_option = _latest_answer_option(conn, int(qid))
        existing = conn.execute(
            """
            SELECT selected_option, is_correct, time_spent_sec, created_at
            FROM ui_qotd_log
            WHERE user_name = ? AND qotd_date = ?
            LIMIT 1
            """,
            (str(user_name), qotd_date),
        ).fetchone()

        return {
            "user_name": user_name,
            "qotd_date": qotd_date,
            "question": _row_to_question(row),
            "correct_option": answer_option,
            "attempted_today": existing is not None,
            "attempt": (
                {
                    "selected_option": existing["selected_option"],
                    "is_correct": existing["is_correct"],
                    "time_spent_sec": existing["time_spent_sec"],
                    "submitted_at": existing["created_at"],
                }
                if existing is not None
                else None
            ),
        }
    finally:
        conn.close()


@app.post("/api/qotd/submit")
def submit_question_of_the_day(request: QOTDSubmitRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        qotd_date = _today_iso()
        expected_qid = _qotd_question_id(conn, user_name=request.user_name, qotd_date=qotd_date)
        if int(request.question_id) != int(expected_qid):
            raise HTTPException(status_code=400, detail="Submitted question is not today's QOTD")

        row = conn.execute("SELECT * FROM questions WHERE id = ?", (int(expected_qid),)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="QOTD question not found")

        selected = _coerce_option(request.selected_option)
        correct_option = _latest_answer_option(conn, int(expected_qid))
        is_correct: bool | None = None
        if selected is not None and correct_option is not None:
            is_correct = int(selected) == int(correct_option)

        question = _row_to_question(row)
        mistake_type = _classify_mistake_type(
            question_text=str(question.get("question_text", "")),
            selected_option=selected,
            correct_option=correct_option,
            time_spent_sec=int(request.time_spent_sec),
        )

        with conn:
            conn.execute(
                """
                INSERT INTO ui_qotd_log(
                    user_name, qotd_date, question_id, selected_option, correct_option, is_correct, time_spent_sec
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_name, qotd_date)
                DO UPDATE SET
                    question_id = excluded.question_id,
                    selected_option = excluded.selected_option,
                    correct_option = excluded.correct_option,
                    is_correct = excluded.is_correct,
                    time_spent_sec = excluded.time_spent_sec,
                    created_at = datetime('now')
                """,
                (
                    str(request.user_name),
                    qotd_date,
                    int(expected_qid),
                    selected,
                    correct_option,
                    None if is_correct is None else (1 if is_correct else 0),
                    int(request.time_spent_sec),
                ),
            )

            conn.execute(
                "DELETE FROM ui_attempts WHERE user_name = ? AND mode = 'qotd' AND DATE(attempted_at) = DATE('now')",
                (str(request.user_name),),
            )

            conn.execute(
                """
                INSERT INTO ui_attempts(
                    user_name, mode, session_id, question_id, selected_option,
                    correct_option, is_correct, time_spent_sec, mistake_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(request.user_name),
                    "qotd",
                    f"qotd-{qotd_date}",
                    int(expected_qid),
                    selected,
                    correct_option,
                    None if is_correct is None else (1 if is_correct else 0),
                    int(request.time_spent_sec),
                    mistake_type,
                ),
            )

        return {
            "status": "ok",
            "user_name": request.user_name,
            "qotd_date": qotd_date,
            "question_id": int(expected_qid),
            "selected_option": selected,
            "correct_option": correct_option,
            "is_correct": is_correct,
            "mistake_type": mistake_type,
        }
    finally:
        conn.close()


@app.post("/api/goals/set")
def set_goal_plan(request: GoalSetRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        exam_day = _parse_iso_date(request.exam_date)
        with conn:
            conn.execute(
                """
                INSERT INTO ui_goals(user_name, target_score, exam_date, daily_question_goal)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_name)
                DO UPDATE SET
                    target_score = excluded.target_score,
                    exam_date = excluded.exam_date,
                    daily_question_goal = excluded.daily_question_goal,
                    updated_at = datetime('now')
                """,
                (
                    str(request.user_name),
                    int(request.target_score),
                    exam_day.isoformat(),
                    int(request.daily_question_goal),
                ),
            )

        return {
            "status": "ok",
            "user_name": request.user_name,
            "target_score": int(request.target_score),
            "exam_date": exam_day.isoformat(),
            "daily_question_goal": int(request.daily_question_goal),
        }
    finally:
        conn.close()


@app.get("/api/revision/plan")
def get_revision_plan(user_name: str = Query(default="default")) -> dict[str, Any]:
    conn = _connect_db()
    try:
        rows = conn.execute(
            """
            SELECT plan_date, topic, tasks, completed
            FROM ui_revision_plan
            WHERE user_name = ?
            ORDER BY plan_date ASC
            """,
            (str(user_name),),
        ).fetchall()

        items: list[dict[str, Any]] = []
        for row in rows:
            try:
                tasks = json.loads(str(row["tasks"] or "[]"))
            except Exception:
                tasks = []
            if not isinstance(tasks, list):
                tasks = [str(tasks)]

            items.append(
                {
                    "date": str(row["plan_date"]),
                    "topic": str(row["topic"]),
                    "tasks": [str(item) for item in tasks],
                    "completed": bool(int(row["completed"] or 0)),
                }
            )

        completion = round(
            (sum(1 for row in items if row.get("completed")) / len(items)) * 100.0,
            2,
        ) if items else 0.0

        return {
            "user_name": user_name,
            "items": items,
            "completion_pct": completion,
        }
    finally:
        conn.close()


@app.post("/api/revision/generate")
def generate_revision_plan(request: RevisionGenerateRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        weak_topics = [str(item).strip() for item in request.weak_topics if str(item).strip()]
        if not weak_topics:
            weak_topics = _weak_topics_for_user(conn, user_name=request.user_name, limit=8)
        if not weak_topics:
            weak_topics = ["General Revision"]

        start_date = date.today()
        end_date = start_date + timedelta(days=max(0, int(request.days) - 1))

        rows: list[dict[str, Any]] = []
        cursor = start_date
        idx = 0
        while cursor <= end_date:
            topic = weak_topics[idx % len(weak_topics)]
            tasks = [
                f"45 min concept revision for {topic}",
                f"Solve {int(request.daily_question_target)} MCQs",
                "15 min error-log review",
            ]
            rows.append(
                {
                    "date": cursor.isoformat(),
                    "topic": topic,
                    "tasks": tasks,
                    "completed": False,
                }
            )
            cursor += timedelta(days=1)
            idx += 1

        with conn:
            conn.execute("DELETE FROM ui_revision_plan WHERE user_name = ?", (request.user_name,))
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO ui_revision_plan(user_name, plan_date, topic, tasks, completed)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        request.user_name,
                        row["date"],
                        row["topic"],
                        json.dumps(row["tasks"], ensure_ascii=False),
                        0,
                    ),
                )

        return {
            "user_name": request.user_name,
            "generated_days": len(rows),
            "items": rows,
        }
    finally:
        conn.close()


@app.post("/api/revision/mark")
def mark_revision_day(request: RevisionMarkRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        with conn:
            conn.execute(
                """
                UPDATE ui_revision_plan
                SET completed = ?
                WHERE user_name = ? AND plan_date = ?
                """,
                (1 if request.completed else 0, request.user_name, request.plan_date),
            )

        return {
            "status": "ok",
            "user_name": request.user_name,
            "plan_date": request.plan_date,
            "completed": bool(request.completed),
        }
    finally:
        conn.close()


@app.post("/api/flashcards/generate")
def generate_flashcards(request: FlashcardsGenerateRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        rows = conn.execute(
            """
            SELECT question_id, COUNT(*) AS wrong_count
            FROM ui_attempts
            WHERE user_name = ? AND is_correct = 0
            GROUP BY question_id
            ORDER BY wrong_count DESC
            LIMIT ?
            """,
            (request.user_name, int(request.limit)),
        ).fetchall()

        added = 0
        with conn:
            for row in rows:
                question_id = int(row["question_id"])
                exists = conn.execute(
                    "SELECT 1 FROM ui_flashcards WHERE user_name = ? AND question_id = ? LIMIT 1",
                    (request.user_name, question_id),
                ).fetchone()
                if exists:
                    continue

                conn.execute(
                    """
                    INSERT INTO ui_flashcards(
                        user_name, question_id, interval_days, ease, next_due, last_reviewed,
                        lapse_count, review_count, is_leech
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request.user_name,
                        question_id,
                        1,
                        2.3,
                        _today_iso(),
                        None,
                        0,
                        0,
                        0,
                    ),
                )
                added += 1

        total_cards = int(
            conn.execute("SELECT COUNT(*) FROM ui_flashcards WHERE user_name = ?", (request.user_name,)).fetchone()[0]
        )
        return {
            "status": "ok",
            "user_name": request.user_name,
            "added": added,
            "total_cards": total_cards,
        }
    finally:
        conn.close()


@app.get("/api/flashcards")
def get_flashcards(
    user_name: str = Query(default="default"),
    due_only: bool = Query(default=False),
    leech_only: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    conn = _connect_db()
    try:
        where = "WHERE f.user_name = ?"
        params: list[Any] = [str(user_name)]

        if due_only:
            where += " AND DATE(f.next_due) <= DATE('now')"

        if leech_only:
            where += " AND COALESCE(f.is_leech, 0) = 1"

        rows = conn.execute(
            f"""
            SELECT
                f.question_id,
                f.interval_days,
                f.ease,
                f.next_due,
                f.last_reviewed,
                COALESCE(f.lapse_count, 0) AS lapse_count,
                COALESCE(f.review_count, 0) AS review_count,
                COALESCE(f.is_leech, 0) AS is_leech,
                q.question_text,
                q.subject,
                q.topic
            FROM ui_flashcards f
            JOIN questions q ON q.id = f.question_id
            {where}
            ORDER BY COALESCE(f.is_leech, 0) DESC, DATE(f.next_due) ASC, f.question_id ASC
            LIMIT ?
            """,
            [*params, int(limit)],
        ).fetchall()

        question_ids = [int(row["question_id"]) for row in rows]
        answer_map = _latest_answer_map(conn, question_ids)

        all_count = int(
            conn.execute("SELECT COUNT(*) FROM ui_flashcards WHERE user_name = ?", (str(user_name),)).fetchone()[0]
        )
        due_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM ui_flashcards WHERE user_name = ? AND DATE(next_due) <= DATE('now')",
                (str(user_name),),
            ).fetchone()[0]
        )
        leech_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM ui_flashcards WHERE user_name = ? AND COALESCE(is_leech, 0) = 1",
                (str(user_name),),
            ).fetchone()[0]
        )

        return {
            "user_name": user_name,
            "total": all_count,
            "due_today": due_count,
            "leech_cards": leech_count,
            "items": [
                {
                    "question_id": int(row["question_id"]),
                    "interval_days": int(row["interval_days"] or 1),
                    "ease": float(row["ease"] or 2.3),
                    "next_due": str(row["next_due"]),
                    "last_reviewed": row["last_reviewed"],
                    "lapse_count": int(row["lapse_count"] or 0),
                    "review_count": int(row["review_count"] or 0),
                    "is_leech": bool(int(row["is_leech"] or 0)),
                    "question_text": str(row["question_text"]),
                    "subject": _normalize_text(row["subject"], "Unknown"),
                    "topic": _normalize_text(row["topic"], "unknown"),
                    "answer_key": answer_map.get(int(row["question_id"])),
                }
                for row in rows
            ],
        }
    finally:
        conn.close()


@app.post("/api/flashcards/review")
def review_flashcard(request: FlashcardReviewRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        card = conn.execute(
            """
            SELECT interval_days, ease, COALESCE(lapse_count, 0) AS lapse_count,
                   COALESCE(review_count, 0) AS review_count,
                   COALESCE(is_leech, 0) AS is_leech
            FROM ui_flashcards
            WHERE user_name = ? AND question_id = ?
            LIMIT 1
            """,
            (request.user_name, int(request.question_id)),
        ).fetchone()

        if card is None:
            raise HTTPException(status_code=404, detail="Flashcard not found")

        rating = str(request.rating or "good").strip().lower()
        interval = int(card["interval_days"] or 1)
        ease = float(card["ease"] or 2.3)
        lapse_count = int(card["lapse_count"] or 0)
        review_count = int(card["review_count"] or 0)

        topic_accuracy = _topic_accuracy_for_question(conn, user_name=request.user_name, question_id=int(request.question_id))
        review_count += 1

        if rating == "again":
            new_interval = 1
            new_ease = max(1.3, ease - 0.25)
            lapse_count += 1
        elif rating == "easy":
            boost = 1.8 if topic_accuracy >= 75.0 else 1.5
            new_interval = max(3, int(round((interval * boost) + 2)))
            new_ease = min(3.2, ease + 0.10)
        else:
            boost = 1.35 if topic_accuracy >= 75.0 else 1.15
            new_interval = max(2, int(round((interval * boost) + 1)))
            new_ease = min(3.0, ease + 0.02)

        is_leech = 1 if lapse_count >= 3 and review_count >= 4 else 0
        if is_leech and rating != "again":
            new_interval = min(new_interval, 4)

        next_due = (date.today() + timedelta(days=int(new_interval))).isoformat()
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        with conn:
            conn.execute(
                """
                UPDATE ui_flashcards
                SET interval_days = ?, ease = ?, next_due = ?, last_reviewed = ?,
                    lapse_count = ?, review_count = ?, is_leech = ?
                WHERE user_name = ? AND question_id = ?
                """,
                (
                    int(new_interval),
                    round(float(new_ease), 2),
                    next_due,
                    now_iso,
                    int(lapse_count),
                    int(review_count),
                    int(is_leech),
                    request.user_name,
                    int(request.question_id),
                ),
            )

        return {
            "status": "ok",
            "question_id": int(request.question_id),
            "rating": rating,
            "interval_days": int(new_interval),
            "ease": round(float(new_ease), 2),
            "next_due": next_due,
            "lapse_count": int(lapse_count),
            "review_count": int(review_count),
            "is_leech": bool(is_leech),
            "topic_accuracy": float(topic_accuracy),
        }
    finally:
        conn.close()


@app.get("/api/verification/snapshot")
def verification_snapshot() -> dict[str, Any]:
    return run_project_verification(
        project_root=Path("."),
        deep_pdf_scan=False,
        pdf_sample_limit=20,
        verify_remote_sources=False,
    )


@app.post("/api/verification/run")
def verification_run(config: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    return run_project_verification(
        project_root=Path("."),
        deep_pdf_scan=bool(config.get("deep_pdf_scan", False)),
        pdf_sample_limit=int(config.get("pdf_sample_limit", 20) or 20),
        verify_remote_sources=bool(config.get("verify_remote_sources", False)),
        remote_sample_limit=int(config.get("remote_sample_limit", 0) or 0),
        remote_timeout_seconds=int(config.get("remote_timeout_seconds", 20) or 20),
    )


@app.post("/api/ai/ask")
def ai_ask(request: AIAskRequest) -> dict[str, Any]:
    prompt = str(request.prompt or "").strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="prompt is required")

    context = str(request.context or "").strip()

    reply = _run_groq_chat(
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a NEET mentor. Be practical, concise, and student-friendly. "
                    "Give clear steps and avoid generic fluff."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Student profile: {request.user_name}\n"
                    f"Context: {context or 'No additional context'}\n\n"
                    f"Question: {prompt}"
                ),
            },
        ],
        model=DEFAULT_AI_MODEL,
        temperature=0.3,
        max_tokens=1000,
    )

    return {
        "user_name": request.user_name,
        "reply": reply,
    }


@app.post("/api/ai/explain")
def ai_explain(request: AIExplainRequest) -> dict[str, Any]:
    conn = _connect_db()
    try:
        row = conn.execute("SELECT * FROM questions WHERE id = ?", (int(request.question_id),)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Question not found")

        question = _row_to_question(row)
        correct_option = _latest_answer_option(conn, int(request.question_id))

        option_lines = []
        for idx, option in enumerate(question.get("options", []), start=1):
            option_lines.append(f"{idx}. {option}")

        explain_text = _run_groq_chat(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a NEET tutor. Explain in simple language, then show the exact solving steps. "
                        "Add one quick exam tip at the end."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Question ID: {request.question_id}\n"
                        f"Subject: {question.get('subject', 'Unknown')}\n"
                        f"Topic: {question.get('topic', 'unknown')}\n"
                        f"Question: {question.get('question_text', '')}\n"
                        f"Options:\n" + "\n".join(option_lines) + "\n\n"
                        f"Student selected option: {request.selected_option}\n"
                        f"Official answer key option: {correct_option}"
                    ),
                },
            ],
            model=DEFAULT_AI_MODEL,
            temperature=0.2,
            max_tokens=1100,
        )

        return {
            "question_id": int(request.question_id),
            "correct_option": correct_option,
            "explanation": explain_text,
        }
    finally:
        conn.close()
