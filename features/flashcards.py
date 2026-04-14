from __future__ import annotations

import csv
import json
import re
from pathlib import Path
import sqlite3
from typing import Any

from groq import Groq

MODEL_NAME = "llama-3.3-70b-versatile"


def _fetch_question(conn: sqlite3.Connection, question_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM questions WHERE id = ?", (int(question_id),)).fetchone()
    if row is None:
        raise ValueError(f"Question not found: {question_id}")
    item = dict(row)
    item["options"] = json.loads(item.get("options") or "[]")
    return item


def _fetch_answer_row(conn: sqlite3.Connection, question_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM answer_keys WHERE question_id = ? ORDER BY id DESC LIMIT 1",
        (int(question_id),),
    ).fetchone()
    return dict(row) if row else None


def _build_front(question: dict[str, Any]) -> str:
    front = question.get("question_text", "").strip()
    options = question.get("options", [])
    if isinstance(options, list) and options:
        option_lines = [f"{idx}. {str(value).strip()}" for idx, value in enumerate(options, start=1)]
        front = f"{front}\n\n" + "\n".join(option_lines)
    return front


def _parse_json_payload(text: str) -> dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def _generate_ai_answer(question: dict[str, Any], client: Groq) -> dict[str, str]:
    prompt = json.dumps(
        {
            "question_text": question.get("question_text", ""),
            "options": question.get("options", []),
            "question_type": question.get("question_type", "unknown"),
            "required": ["answer", "explanation", "hint"],
        },
        ensure_ascii=False,
    )
    response = client.chat.completions.create(
        model=MODEL_NAME,
        temperature=0.2,
        max_tokens=1024,
        messages=[
            {"role": "system", "content": "Respond only as JSON with keys: answer, explanation, hint."},
            {"role": "user", "content": prompt},
        ],
    )
    text = str((response.choices[0].message.content if response.choices else "") or "")
    payload = _parse_json_payload(text)
    return {
        "answer": str(payload.get("answer", "unknown")),
        "explanation": str(payload.get("explanation", "No explanation generated.")),
        "hint": str(payload.get("hint", "Review core concept and retry.")),
    }


def generate_flashcard(
    question_id: int,
    conn: sqlite3.Connection,
    client: Groq | None,
) -> dict[str, Any]:
    """Generate one flashcard from a DB question and answer source."""
    question = _fetch_question(conn, question_id)
    answer_row = _fetch_answer_row(conn, question_id)

    if answer_row:
        answer = str(answer_row.get("answer", "unknown"))
        explanation = str(answer_row.get("explanation", ""))
        hint = "Focus on first principles before options elimination."
    else:
        if client is None:
            answer = "unknown"
            explanation = "No stored answer is available and AI generation is disabled."
            hint = "Review concept and eliminate options before final selection."
        else:
            ai_payload = _generate_ai_answer(question, client)
            answer = ai_payload["answer"]
            explanation = ai_payload["explanation"]
            hint = ai_payload["hint"]

    tags = [
        str(question.get("subject", "")).strip(),
        str(question.get("topic", "")).strip(),
        str(question.get("subtopic", "")).strip(),
        str(question.get("difficulty", "")).strip(),
    ]
    tags = [tag for tag in tags if tag and tag.lower() != "unknown"]

    return {
        "question_id": int(question_id),
        "front": _build_front(question),
        "back": f"Answer: {answer}\n\n{explanation}".strip(),
        "tags": tags,
        "hint": hint,
    }


def generate_flashcard_deck(
    filters: dict[str, Any],
    conn: sqlite3.Connection,
    client: Groq | None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Generate a flashcard deck from filtered question rows."""
    filters = filters or {}
    clauses: list[str] = []
    params: list[Any] = []

    for key in ("subject", "topic", "subtopic", "difficulty", "source_year"):
        value = filters.get(key)
        if value is None or value == "":
            continue
        clauses.append(f"{key} = ?")
        params.append(value)

    query = "SELECT id FROM questions"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY id LIMIT ?"
    params.append(max(1, int(limit)))

    ids = [int(row[0]) for row in conn.execute(query, params).fetchall()]
    return [generate_flashcard(question_id=item, conn=conn, client=client) for item in ids]


def export_to_csv(cards: list[dict[str, Any]], output_path: str) -> None:
    """Export flashcards to CSV format."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["question_id", "front", "back", "tags", "hint"])
        writer.writeheader()
        for card in cards:
            writer.writerow({**card, "tags": ", ".join(card.get("tags", []))})


def export_to_anki_txt(cards: list[dict[str, Any]], output_path: str) -> None:
    """Export flashcards to Anki tab-separated format (front TAB back)."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for card in cards:
            front = str(card.get("front", "")).replace("\t", " ").replace("\n", "<br>")
            back = str(card.get("back", "")).replace("\t", " ").replace("\n", "<br>")
            handle.write(f"{front}\t{back}\n")
