from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from groq import Groq
import pdfplumber

PARSE_MODEL = "llama-3.3-70b-versatile"
EXPLAIN_MODEL = "llama-3.3-70b-versatile"


def _extract_text(pdf_path: str) -> str:
    chunks: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = (page.extract_text(layout=True) or "").strip()
            if text:
                chunks.append(text)
    return "\n".join(chunks)


def _question_number_from_text(text: str) -> int | None:
    patterns = [
        r"^\s*Q\.?\s*(\d+)\b",
        r"^\s*(\d+)[\.)]\s+",
        r"^\s*Question\s+(\d+)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _latest_answer_row(conn: sqlite3.Connection, question_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT answer, explanation, source FROM answer_keys WHERE question_id = ? ORDER BY id DESC LIMIT 1",
        (int(question_id),),
    ).fetchone()
    return dict(row) if row else None


def _parse_json_payload(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}|\[.*\]", text, flags=re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def import_answer_key_from_pdf(
    pdf_path: str,
    conn: sqlite3.Connection,
    client: Groq,
) -> int:
    """Parse answers from an answer-key PDF and map them into the DB."""
    text = _extract_text(pdf_path)
    if not text.strip():
        return 0

    year_match = re.search(r"(19\d{2}|20\d{2})", Path(pdf_path).name)
    source_year = int(year_match.group(1)) if year_match else None

    prompt = (
        "Extract answer keys from this exam text. "
        "Return only JSON array of objects with fields: question_number (int), answer (string).\n\n"
        f"TEXT:\n{text[:24000]}"
    )

    response = client.chat.completions.create(
        model=PARSE_MODEL,
        temperature=0.0,
        max_tokens=1024,
        messages=[
            {"role": "system", "content": "Return only JSON array: [{question_number:int, answer:string}]"},
            {"role": "user", "content": prompt},
        ],
    )

    response_text = str((response.choices[0].message.content if response.choices else "") or "")
    parsed = _parse_json_payload(response_text)
    if not isinstance(parsed, list):
        return 0

    questions = conn.execute(
        "SELECT id, question_text, source_year FROM questions WHERE (? IS NULL OR source_year = ?)",
        (source_year, source_year),
    ).fetchall()

    qnum_to_id: dict[int, int] = {}
    for row in questions:
        qnum = _question_number_from_text(str(row[1] or ""))
        if qnum is not None and qnum not in qnum_to_id:
            qnum_to_id[qnum] = int(row[0])

    inserted = 0
    with conn:
        for item in parsed:
            qnum = int(item.get("question_number", 0) or 0)
            answer = str(item.get("answer", "")).strip()
            if qnum <= 0 or not answer:
                continue

            question_id = qnum_to_id.get(qnum)
            if question_id is None:
                logging.warning("Unmatched answer key entry: Q%s", qnum)
                continue

            conn.execute(
                "INSERT INTO answer_keys(question_id, answer, explanation, source) VALUES (?, ?, ?, ?)",
                (question_id, answer, "", "scraped"),
            )
            inserted += 1

    return inserted


def generate_ai_explanation(
    question_id: int,
    conn: sqlite3.Connection,
    client: Groq,
) -> dict[str, Any]:
    """Generate AI explanation, persist it, and return parsed explanation fields."""
    row = conn.execute("SELECT * FROM questions WHERE id = ?", (int(question_id),)).fetchone()
    if row is None:
        raise ValueError(f"Question not found: {question_id}")

    question = dict(row)
    options = json.loads(question.get("options") or "[]")

    prompt = json.dumps(
        {
            "question_id": int(question_id),
            "question_text": question.get("question_text", ""),
            "options": options,
            "question_type": question.get("question_type", "unknown"),
            "required_format": {
                "answer": "string",
                "step_by_step_solution": ["step 1", "step 2"],
                "key_concept": "string",
                "common_mistake": "string",
            },
        },
        ensure_ascii=False,
    )

    response = client.chat.completions.create(
        model=EXPLAIN_MODEL,
        temperature=0.2,
        max_tokens=2048,
        messages=[
            {
                "role": "system",
                "content": (
                    "Respond only with valid JSON containing: "
                    "answer, step_by_step_solution, key_concept, common_mistake"
                ),
            },
            {"role": "user", "content": prompt},
        ],
    )

    response_text = str((response.choices[0].message.content if response.choices else "") or "")
    payload = _parse_json_payload(response_text)
    answer = str(payload.get("answer", "unknown"))

    with conn:
        conn.execute(
            "INSERT INTO answer_keys(question_id, answer, explanation, source) VALUES (?, ?, ?, ?)",
            (int(question_id), answer, json.dumps(payload, ensure_ascii=False), "ai_generated"),
        )

    return payload


def get_answer(question_id: int, conn: sqlite3.Connection) -> dict[str, Any] | None:
    """Fetch the most recent answer-key row for a question id."""
    row = _latest_answer_row(conn, question_id)
    if row is None:
        return None

    explanation_raw = row.get("explanation") or ""
    explanation_payload: Any = explanation_raw
    try:
        explanation_payload = json.loads(explanation_raw)
    except Exception:
        pass

    return {
        "question_id": int(question_id),
        "answer": row.get("answer"),
        "explanation": explanation_payload,
        "source": row.get("source"),
    }
