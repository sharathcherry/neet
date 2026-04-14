from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import random
import sqlite3
from typing import Any
from uuid import uuid4


def _normalized_mix(config: dict[str, Any]) -> dict[str, float]:
    base = {"easy": 0.3, "medium": 0.5, "hard": 0.2}
    custom = config.get("difficulty_mix") or {}
    for key in base:
        if key in custom:
            base[key] = float(custom[key])
    total = sum(base.values()) or 1.0
    return {key: value / total for key, value in base.items()}


def _load_candidates(conn: sqlite3.Connection, config: dict[str, Any]) -> list[dict[str, Any]]:
    query = (
        "SELECT q.*, (SELECT answer FROM answer_keys a WHERE a.question_id = q.id ORDER BY a.id DESC LIMIT 1) AS answer "
        "FROM questions q"
    )
    clauses: list[str] = []
    params: list[Any] = []

    if config.get("subject"):
        clauses.append("q.subject = ?")
        params.append(config["subject"])

    topics = config.get("topics") or []
    if topics:
        placeholders = ",".join(["?" for _ in topics])
        clauses.append(f"q.topic IN ({placeholders})")
        params.extend(topics)

    years = config.get("years_range") or []
    if len(years) == 2:
        clauses.append("q.source_year BETWEEN ? AND ?")
        params.extend([int(years[0]), int(years[1])])

    types = config.get("question_types") or []
    if types:
        placeholders = ",".join(["?" for _ in types])
        clauses.append(f"q.question_type IN ({placeholders})")
        params.extend(types)

    if clauses:
        query += " WHERE " + " AND ".join(clauses)

    rows = conn.execute(query, params).fetchall()
    exclude_ids = {int(item) for item in (config.get("exclude_question_ids") or [])}

    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["options"] = json.loads(item.get("options") or "[]")
        if int(item.get("id", 0)) in exclude_ids:
            continue
        result.append(item)
    return result


def _sample_by_difficulty(pool: list[dict[str, Any]], count: int, mix: dict[str, float]) -> list[dict[str, Any]]:
    by_diff: dict[str, list[dict[str, Any]]] = {"easy": [], "medium": [], "hard": [], "unknown": []}
    for item in pool:
        diff = str(item.get("difficulty", "unknown")).lower()
        by_diff.setdefault(diff, []).append(item)

    selected: list[dict[str, Any]] = []
    selected_ids: set[int] = set()

    for diff in ("easy", "medium", "hard"):
        target = int(round(count * mix.get(diff, 0.0)))
        candidates = by_diff.get(diff, [])
        random.shuffle(candidates)
        for item in candidates[:target]:
            qid = int(item["id"])
            if qid not in selected_ids:
                selected.append(item)
                selected_ids.add(qid)

    if len(selected) < count:
        remaining = [item for item in pool if int(item["id"]) not in selected_ids]
        random.shuffle(remaining)
        selected.extend(remaining[: count - len(selected)])

    return selected[:count]


def generate_mock_paper(config: dict[str, Any], conn: sqlite3.Connection) -> dict[str, Any]:
    """Generate a mock paper by sampling questions from SQLite."""
    config = dict(config or {})
    total_questions = max(1, int(config.get("total_questions", 30)))
    mix = _normalized_mix(config)
    candidates = _load_candidates(conn, config)

    if not candidates:
        logging.warning("No candidates found for mock paper config")

    topics = config.get("topics") or []
    selected: list[dict[str, Any]] = []

    if topics:
        per_topic_target = max(1, total_questions // max(1, len(topics)))
        used_ids: set[int] = set()
        for topic in topics:
            topic_pool = [item for item in candidates if str(item.get("topic", "")) == str(topic)]
            topic_selected = _sample_by_difficulty(topic_pool, per_topic_target, mix)
            for item in topic_selected:
                qid = int(item["id"])
                if qid not in used_ids:
                    selected.append(item)
                    used_ids.add(qid)

    if len(selected) < total_questions:
        used_ids = {int(item["id"]) for item in selected}
        remaining = [item for item in candidates if int(item["id"]) not in used_ids]
        selected.extend(_sample_by_difficulty(remaining, total_questions - len(selected), mix))

    if len(selected) < total_questions:
        logging.warning("Requested %s questions but only %s available", total_questions, len(selected))

    paper = {
        "paper_id": str(uuid4()),
        "questions": selected[:total_questions],
        "config_used": config,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return paper


def export_paper_to_txt(paper: dict[str, Any], output_path: str) -> None:
    """Export a generated mock paper to a human-readable TXT with answer-key page."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = [f"Mock Paper ID: {paper.get('paper_id', '')}", ""]
    answers: list[str] = []

    for index, question in enumerate(paper.get("questions", []), start=1):
        lines.append(f"Q{index}. {question.get('question_text', '')}")
        for opt_index, option in enumerate(question.get("options", []), start=1):
            lines.append(f"  {opt_index}. {option}")
        lines.append("")
        answers.append(f"Q{index}: {question.get('answer', 'NA')}")

    lines.extend(["", "Answer Key", "----------", *answers])
    path.write_text("\n".join(lines), encoding="utf-8")


def export_paper_to_json(paper: dict[str, Any], output_path: str) -> None:
    """Export a generated mock paper to JSON."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(paper, indent=2), encoding="utf-8")
