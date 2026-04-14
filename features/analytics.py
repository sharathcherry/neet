from __future__ import annotations

import sqlite3
from typing import Any

from pipeline.ingestor import log_attempt as ingest_log_attempt


def log_attempt(question_id: int, session_id: str, is_correct: bool, conn: sqlite3.Connection) -> None:
    """Log one attempt via the shared ingestion helper."""
    ingest_log_attempt(conn=conn, question_id=question_id, session_id=session_id, is_correct=is_correct)


def get_weak_topics(
    session_id: str,
    conn: sqlite3.Connection,
    min_attempts: int = 3,
) -> list[dict[str, Any]]:
    """Return low-accuracy topics for one session using SQL aggregation."""
    rows = conn.execute(
        """
        SELECT
            q.topic,
            COALESCE(q.subtopic, 'unknown') AS subtopic,
            COUNT(*) AS total_attempts,
            SUM(a.is_correct) AS correct,
            ROUND(100.0 * AVG(a.is_correct), 1) AS accuracy_pct
        FROM attempts a
        JOIN questions q ON q.id = a.question_id
        WHERE a.session_id = ?
        GROUP BY q.topic, COALESCE(q.subtopic, 'unknown')
        HAVING COUNT(*) >= ? AND (100.0 * AVG(a.is_correct)) < 60.0
        ORDER BY accuracy_pct ASC, total_attempts DESC
        """,
        (str(session_id), max(1, int(min_attempts))),
    ).fetchall()

    return [
        {
            "topic": row[0],
            "subtopic": row[1],
            "total_attempts": int(row[2] or 0),
            "correct": int(row[3] or 0),
            "accuracy_pct": float(row[4] or 0.0),
        }
        for row in rows
    ]


def get_session_summary(session_id: str, conn: sqlite3.Connection) -> dict[str, Any]:
    """Return aggregated session performance including weak-topic recommendations."""
    totals = conn.execute(
        """
        SELECT COUNT(*) AS total_attempted, SUM(is_correct) AS total_correct, ROUND(100.0 * AVG(is_correct), 1) AS overall_accuracy
        FROM attempts
        WHERE session_id = ?
        """,
        (str(session_id),),
    ).fetchone()

    by_subject_rows = conn.execute(
        """
        SELECT q.subject, COUNT(*) AS attempted, SUM(a.is_correct) AS correct, ROUND(100.0 * AVG(a.is_correct), 1) AS accuracy_pct
        FROM attempts a
        JOIN questions q ON q.id = a.question_id
        WHERE a.session_id = ?
        GROUP BY q.subject
        ORDER BY accuracy_pct ASC
        """,
        (str(session_id),),
    ).fetchall()

    weak_topics = get_weak_topics(session_id=session_id, conn=conn, min_attempts=3)
    recommended = [item["topic"] for item in weak_topics[:3]]

    return {
        "session_id": str(session_id),
        "total_attempted": int((totals[0] if totals else 0) or 0),
        "total_correct": int((totals[1] if totals else 0) or 0),
        "overall_accuracy_pct": float((totals[2] if totals else 0.0) or 0.0),
        "by_subject": [
            {
                "subject": row[0],
                "attempted": int(row[1] or 0),
                "correct": int(row[2] or 0),
                "accuracy_pct": float(row[3] or 0.0),
            }
            for row in by_subject_rows
        ],
        "weak_topics": weak_topics,
        "recommended_topics": recommended,
    }


def get_global_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return global aggregate stats: hardest questions, easiest topics, and most attempted."""
    hardest_rows = conn.execute(
        """
        SELECT q.id, q.topic, q.subtopic, COUNT(*) AS attempts, ROUND(100.0 * AVG(a.is_correct), 1) AS accuracy_pct
        FROM attempts a
        JOIN questions q ON q.id = a.question_id
        GROUP BY q.id, q.topic, q.subtopic
        HAVING COUNT(*) >= 3
        ORDER BY accuracy_pct ASC, attempts DESC
        LIMIT 10
        """
    ).fetchall()

    easiest_topic_rows = conn.execute(
        """
        SELECT q.topic, COUNT(*) AS attempts, ROUND(100.0 * AVG(a.is_correct), 1) AS accuracy_pct
        FROM attempts a
        JOIN questions q ON q.id = a.question_id
        GROUP BY q.topic
        HAVING COUNT(*) >= 3
        ORDER BY accuracy_pct DESC, attempts DESC
        LIMIT 10
        """
    ).fetchall()

    most_attempted_rows = conn.execute(
        """
        SELECT q.id, q.topic, q.subtopic, COUNT(*) AS attempts
        FROM attempts a
        JOIN questions q ON q.id = a.question_id
        GROUP BY q.id, q.topic, q.subtopic
        ORDER BY attempts DESC
        LIMIT 10
        """
    ).fetchall()

    return {
        "hardest_questions": [
            {
                "question_id": int(row[0]),
                "topic": row[1],
                "subtopic": row[2],
                "attempts": int(row[3] or 0),
                "accuracy_pct": float(row[4] or 0.0),
            }
            for row in hardest_rows
        ],
        "easiest_topics": [
            {"topic": row[0], "attempts": int(row[1] or 0), "accuracy_pct": float(row[2] or 0.0)}
            for row in easiest_topic_rows
        ],
        "most_attempted": [
            {"question_id": int(row[0]), "topic": row[1], "subtopic": row[2], "attempts": int(row[3] or 0)}
            for row in most_attempted_rows
        ],
    }
