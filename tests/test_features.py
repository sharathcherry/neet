from __future__ import annotations

import sqlite3

from features.analytics import get_session_summary, log_attempt
from features.mock_paper import generate_mock_paper
from features.topic_bank import get_topic_stats, get_topic_tree
from pipeline.ingestor import init_db, insert_answer, insert_question


def _seed(conn: sqlite3.Connection) -> list[int]:
    ids: list[int] = []
    ids.append(
        insert_question(
            conn,
            {
                "question_text": "Q.1 What is force?",
                "question_type": "short_answer",
                "options": [],
                "subject": "Physics",
                "topic": "Mechanics",
                "subtopic": "Force",
                "difficulty": "easy",
                "bloom_level": "remember",
                "tag_confidence": 0.9,
                "source_year": 2020,
                "source_pdf": "a.pdf",
                "page_hint": 1,
            },
        )
    )
    ids.append(
        insert_question(
            conn,
            {
                "question_text": "Q.2 Calculate current in a 2 ohm resistor.",
                "question_type": "mcq",
                "options": ["1A", "2A", "3A", "4A"],
                "subject": "Physics",
                "topic": "Electricity",
                "subtopic": "Current",
                "difficulty": "medium",
                "bloom_level": "apply",
                "tag_confidence": 0.8,
                "source_year": 2021,
                "source_pdf": "b.pdf",
                "page_hint": 2,
            },
        )
    )
    for qid in ids:
        insert_answer(conn, qid, "B", "sample explanation", "manual")
    return ids


def test_topic_tree_and_stats() -> None:
    conn = init_db(":memory:")
    try:
        _seed(conn)
        tree = get_topic_tree(conn)
        stats = get_topic_stats(conn)
        assert "Physics" in tree
        assert len(stats) >= 1
    finally:
        conn.close()


def test_mock_paper_generation() -> None:
    conn = init_db(":memory:")
    try:
        _seed(conn)
        paper = generate_mock_paper({"subject": "Physics", "total_questions": 2}, conn)
        assert len(paper["questions"]) == 2
        assert paper["paper_id"]
    finally:
        conn.close()


def test_analytics_summary() -> None:
    conn = init_db(":memory:")
    try:
        qids = _seed(conn)
        log_attempt(qids[0], "S1", True, conn)
        log_attempt(qids[1], "S1", False, conn)
        summary = get_session_summary("S1", conn)
        assert summary["total_attempted"] == 2
        assert isinstance(summary["by_subject"], list)
    finally:
        conn.close()
