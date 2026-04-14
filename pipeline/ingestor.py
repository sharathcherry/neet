from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS questions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    question_text   TEXT NOT NULL,
    question_type   TEXT,
    options         TEXT,
    subject         TEXT,
    topic           TEXT,
    subtopic        TEXT,
    difficulty      TEXT,
    bloom_level     TEXT,
    tag_confidence  REAL,
    source_year     INTEGER,
    source_pdf      TEXT,
    page_hint       INTEGER,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS answer_keys (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    question_id     INTEGER REFERENCES questions(id),
    answer          TEXT,
    explanation     TEXT,
    source          TEXT
);

CREATE TABLE IF NOT EXISTS attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    question_id     INTEGER REFERENCES questions(id),
    session_id      TEXT,
    is_correct      INTEGER,
    attempted_at    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_questions_topic ON questions(topic);
CREATE INDEX IF NOT EXISTS idx_questions_subject ON questions(subject);
CREATE INDEX IF NOT EXISTS idx_questions_difficulty ON questions(difficulty);
CREATE INDEX IF NOT EXISTS idx_questions_source_year ON questions(source_year);
"""


def _serialize_options(options: list[str] | None) -> str:
    return json.dumps(options or [], ensure_ascii=False)


def _deserialize_options(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


def _row_to_question(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["options"] = _deserialize_options(item.get("options"))
    return item


def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite schema and return an open connection."""
    path = Path(db_path)
    if str(path) != ":memory:":
        path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    with conn:
        conn.executescript(SCHEMA_SQL)
    return conn


def question_exists(conn: sqlite3.Connection, question_text: str, source_year: int) -> bool:
    """Return True if a question already exists by text and source year."""
    row = conn.execute(
        "SELECT 1 FROM questions WHERE question_text = ? AND source_year = ? LIMIT 1",
        (question_text, int(source_year or 0)),
    ).fetchone()
    return row is not None


def insert_question(conn: sqlite3.Connection, question: dict[str, Any]) -> int:
    """Insert one question after dedupe check and return the new question id or 0 if duplicate."""
    question_text = str(question.get("question_text", "")).strip()
    source_year = int(question.get("source_year", 0) or 0)
    if not question_text:
        return 0
    if question_exists(conn, question_text, source_year):
        return 0

    with conn:
        cursor = conn.execute(
            """
            INSERT INTO questions (
                question_text, question_type, options, subject, topic, subtopic,
                difficulty, bloom_level, tag_confidence, source_year, source_pdf, page_hint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                question_text,
                question.get("question_type"),
                _serialize_options(question.get("options", [])),
                question.get("subject"),
                question.get("topic"),
                question.get("subtopic"),
                question.get("difficulty"),
                question.get("bloom_level"),
                float(question.get("tag_confidence", 0.0) or 0.0),
                source_year,
                question.get("source_pdf"),
                question.get("page_hint"),
            ),
        )
    return int(cursor.lastrowid)


def insert_questions_batch(conn: sqlite3.Connection, questions: list[dict[str, Any]]) -> int:
    """Insert many questions and return inserted count (duplicates are skipped)."""
    inserted = 0
    for question in questions:
        inserted += 1 if insert_question(conn, question) else 0
    return inserted


def get_questions(conn: sqlite3.Connection, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Fetch questions with optional column filters for topic/subject/year/difficulty."""
    filters = filters or {}
    query = "SELECT * FROM questions"
    clauses: list[str] = []
    params: list[Any] = []

    for key, value in filters.items():
        if value is None or value == "":
            continue
        if isinstance(value, list):
            placeholders = ",".join(["?" for _ in value])
            clauses.append(f"{key} IN ({placeholders})")
            params.extend(value)
        else:
            clauses.append(f"{key} = ?")
            params.append(value)

    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY id"

    rows = conn.execute(query, params).fetchall()
    return [_row_to_question(row) for row in rows]


def insert_answer(
    conn: sqlite3.Connection,
    question_id: int,
    answer: str,
    explanation: str,
    source: str,
) -> None:
    """Insert an answer key row for a question."""
    with conn:
        conn.execute(
            "INSERT INTO answer_keys(question_id, answer, explanation, source) VALUES (?, ?, ?, ?)",
            (int(question_id), answer, explanation, source),
        )


def log_attempt(conn: sqlite3.Connection, question_id: int, session_id: str, is_correct: bool) -> None:
    """Log one attempt row with correctness as 0/1 integer."""
    with conn:
        conn.execute(
            "INSERT INTO attempts(question_id, session_id, is_correct) VALUES (?, ?, ?)",
            (int(question_id), str(session_id), 1 if bool(is_correct) else 0),
        )
