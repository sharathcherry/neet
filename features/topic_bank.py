from __future__ import annotations

import json
import sqlite3
from typing import Any


def _fts5_available(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT sqlite_compileoption_used('ENABLE_FTS5')").fetchone()
    return bool(row and int(row[0]) == 1)


def _row_to_question(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["options"] = json.loads(item.get("options") or "[]")
    return item


def get_topic_tree(conn: sqlite3.Connection) -> dict[str, dict[str, dict[str, int]]]:
    """Return nested subject -> topic -> subtopic -> question_count mapping."""
    rows = conn.execute(
        """
        SELECT subject, topic, COALESCE(subtopic, 'unknown') AS subtopic, COUNT(*) AS question_count
        FROM questions
        GROUP BY subject, topic, COALESCE(subtopic, 'unknown')
        ORDER BY subject, topic, subtopic
        """
    ).fetchall()

    tree: dict[str, dict[str, dict[str, int]]] = {}
    for row in rows:
        subject = str(row[0] or "unknown")
        topic = str(row[1] or "unknown")
        subtopic = str(row[2] or "unknown")
        count = int(row[3] or 0)
        tree.setdefault(subject, {}).setdefault(topic, {})[subtopic] = count
    return tree


def search_questions(
    conn: sqlite3.Connection,
    query: str = "",
    filters: dict[str, Any] | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Search question text and apply dynamic filters on question columns."""
    filters = filters or {}
    limit = max(1, int(limit))

    where_clauses: list[str] = []
    params: list[Any] = []

    for key, value in filters.items():
        if value is None or value == "":
            continue
        where_clauses.append(f"q.{key} = ?")
        params.append(value)

    use_fts = bool(query.strip()) and _fts5_available(conn)
    if use_fts:
        conn.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS questions_fts USING fts5(question_text, content='questions', content_rowid='id')"
        )
        conn.execute("INSERT OR REPLACE INTO questions_fts(rowid, question_text) SELECT id, question_text FROM questions")
        base = "SELECT q.* FROM questions q JOIN questions_fts fts ON fts.rowid = q.id WHERE fts.question_text MATCH ?"
        params = [query.strip()] + params
    else:
        base = "SELECT q.* FROM questions q"
        if query.strip():
            where_clauses.append("q.question_text LIKE ?")
            params.append(f"%{query.strip()}%")

    if where_clauses:
        joiner = " AND " if " WHERE " in base else " WHERE "
        base += joiner + " AND ".join(where_clauses)

    base += " ORDER BY q.id DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(base, params).fetchall()
    return [_row_to_question(row) for row in rows]


def get_topic_stats(conn: sqlite3.Connection, subject: str | None = None) -> list[dict[str, Any]]:
    """Return topic/subtopic stats with total and difficulty counts."""
    query = (
        "SELECT topic, COALESCE(subtopic, 'unknown') AS subtopic, "
        "COUNT(*) AS total_questions, "
        "SUM(CASE WHEN LOWER(difficulty)='easy' THEN 1 ELSE 0 END) AS easy_count, "
        "SUM(CASE WHEN LOWER(difficulty)='medium' THEN 1 ELSE 0 END) AS medium_count, "
        "SUM(CASE WHEN LOWER(difficulty)='hard' THEN 1 ELSE 0 END) AS hard_count "
        "FROM questions"
    )
    params: list[Any] = []
    if subject:
        query += " WHERE subject = ?"
        params.append(subject)

    query += " GROUP BY topic, COALESCE(subtopic, 'unknown') ORDER BY total_questions DESC"
    rows = conn.execute(query, params).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "topic": row[0],
                "subtopic": row[1],
                "total_questions": int(row[2] or 0),
                "easy_count": int(row[3] or 0),
                "medium_count": int(row[4] or 0),
                "hard_count": int(row[5] or 0),
            }
        )
    return result
