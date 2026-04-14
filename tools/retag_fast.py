from __future__ import annotations

import argparse
import itertools
import json
import logging
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from groq import Groq

MODEL = "llama-3.1-8b-instant"
BATCH_SIZE = 10
COMMIT_EVERY = 100

SYSTEM_PROMPT = """You are an expert classifier for NEET / AIIMS (Indian medical entrance) questions.
These questions cover Physics, Chemistry, Botany, and Zoology at NCERT Class 11-12 level.

Respond with ONLY a valid JSON array — no markdown fences, no explanation text, no preamble.

Each element must have exactly these keys:
    "id"             — integer, same id as given in the question
    "topic"          — NCERT chapter name (e.g. "Laws of Motion", "Cell: The Unit of Life")
    "subtopic"       — specific concept within the topic
    "difficulty"     — "easy" | "medium" | "hard"
    "bloom_level"    — "remember" | "understand" | "apply" | "analyze" | "evaluate" | "create"
    "subject"        — "Physics" | "Chemistry" | "Botany" | "Zoology"
    "tag_confidence" — float 0.0-1.0

Difficulty guide:
    easy   = direct NCERT recall, one fact, definition
    medium = formula use, 2-step reasoning, concept application
    hard   = multi-step, cross-concept, numerical derivation

Return ONLY the JSON array. No other text whatsoever."""

_key_cycle: Any | None = None
_key_lock = threading.Lock()


def _load_api_keys() -> list[str]:
    env_names = [
        "GROQ_API_KEY",
        "GROQ_KEY_1",
        "GROQ_KEY_2",
        "GROQ_KEY_3",
        "GROQ_KEY_4",
        "GROQ_KEY_5",
    ]

    keys: list[str] = []
    for env_name in env_names:
        value = os.getenv(env_name, "").strip()
        if value:
            keys.append(value)

    seen: set[str] = set()
    deduped: list[str] = []
    for key in keys:
        if key not in seen:
            deduped.append(key)
            seen.add(key)

    if not deduped:
        raise RuntimeError("No Groq API keys found in environment")

    logging.info("Loaded %s API key(s)", len(deduped))
    return deduped


def _next_key() -> str:
    global _key_cycle
    if _key_cycle is None:
        raise RuntimeError("API key cycle is not initialized")

    with _key_lock:
        return next(_key_cycle)


def _build_user_prompt(batch: list[dict]) -> str:
    blocks: list[str] = []

    for i, row in enumerate(batch, start=1):
        row_id = int(row.get("id", 0))
        source_year = row.get("source_year", "")
        question_type = row.get("question_type", "")
        question_text = str(row.get("question_text", ""))[:350]

        lines = [
            f"[{i}] id={row_id}",
            f"Year: {source_year} | Type: {question_type}",
            f"Question: {question_text}",
        ]

        options_raw = row.get("options")
        try:
            options = json.loads(options_raw or "[]")
        except Exception:
            options = []

        if isinstance(options, list) and options:
            for option_index, option_value in enumerate(options[:4], start=1):
                lines.append(f"  ({option_index}) {str(option_value)[:80]}")

        blocks.append("\n".join(lines))

    return "\n\n".join(blocks) + "\n\n"


def _call_groq(api_key: str, user_prompt: str) -> list[dict] | None:
    try:
        client = Groq(api_key=api_key)
        response = client.chat.completions.create(
            model=MODEL,
            max_tokens=1200,
            temperature=0.1,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
        text = str((response.choices[0].message.content if response.choices else "") or "")
        cleaned = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed
        return None
    except json.JSONDecodeError:
        return None
    except Exception as exc:
        logging.warning("Groq call failed: %s", exc)
        return None


def _default_tag(row: dict) -> dict:
    return {
        "id": int(row.get("id", 0)),
        "topic": "unknown",
        "subtopic": "unknown",
        "difficulty": "unknown",
        "bloom_level": "remember",
        "subject": "Unknown",
        "tag_confidence": 0.0,
    }


def _validate_batch_result(result: list[dict] | None, batch: list[dict]) -> bool:
    if not isinstance(result, list):
        return False
    if len(result) != len(batch):
        return False

    try:
        expected_ids = {int(row["id"]) for row in batch}
        actual_ids = {int(item.get("id", -1)) for item in result}
    except Exception:
        return False

    return actual_ids == expected_ids


def _tag_batch(batch: list[dict]) -> list[dict]:
    prompt = _build_user_prompt(batch)
    result = _call_groq(_next_key(), prompt)

    if _validate_batch_result(result, batch):
        return list(result or [])

    logging.warning("Batch validation failed for %s row(s); falling back to individual tagging", len(batch))
    fallback_results: list[dict] = []

    for row in batch:
        single_prompt = _build_user_prompt([row])
        single_result = _call_groq(_next_key(), single_prompt)

        if _validate_batch_result(single_result, [row]):
            fallback_results.append(single_result[0])
        else:
            fallback_results.append(_default_tag(row))

        time.sleep(0.1)

    return fallback_results


def _write_tags(db_path: str, tags: list[dict]) -> int:
    conn = sqlite3.connect(db_path)
    success_count = 0

    try:
        cursor = conn.cursor()
        for tag in tags:
            try:
                row_id = int(tag.get("id", 0))
                topic = str(tag.get("topic", "unknown"))[:120]
                subtopic = str(tag.get("subtopic", "unknown"))[:120]
                difficulty = str(tag.get("difficulty", "unknown"))
                bloom_level = str(tag.get("bloom_level", "remember"))
                subject = str(tag.get("subject", "Unknown"))

                confidence_raw = tag.get("tag_confidence", 0.0)
                confidence = max(0.0, min(1.0, float(confidence_raw)))

                cursor.execute(
                    """
                    UPDATE questions
                    SET topic=?, subtopic=?, difficulty=?, bloom_level=?, subject=?, tag_confidence=?
                    WHERE id=?
                    """,
                    (topic, subtopic, difficulty, bloom_level, subject, confidence, row_id),
                )
                if cursor.rowcount > 0:
                    success_count += 1
            except Exception as exc:
                logging.warning("Failed to write tag for id=%s: %s", tag.get("id"), exc)

        conn.commit()
        return success_count
    finally:
        conn.close()


def _fetch_untagged(db_path: str, limit: int | None) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        query = """
        SELECT id, question_text, question_type, options, source_year
        FROM questions
        WHERE tag_confidence = 0.0 OR tag_confidence IS NULL
        ORDER BY id
        """

        params: list[Any] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def run(db_path: str, workers: int, limit: int | None) -> None:
    global _key_cycle

    keys = _load_api_keys()
    with _key_lock:
        _key_cycle = itertools.cycle(keys)

    untagged_rows = _fetch_untagged(db_path, limit)
    total_questions = len(untagged_rows)
    if total_questions == 0:
        logging.info("No untagged rows found")
        return

    workers = max(1, int(workers))
    batches = [untagged_rows[index : index + BATCH_SIZE] for index in range(0, total_questions, BATCH_SIZE)]
    batch_count = len(batches)
    key_count = len(keys)

    logging.info(
        "Starting fast retag | questions=%s batches=%s model=%s workers=%s keys=%s",
        total_questions,
        batch_count,
        MODEL,
        workers,
        key_count,
    )

    estimated_seconds = (batch_count / (workers * key_count)) * 0.8
    logging.info("Estimated time: %.2f seconds", estimated_seconds)

    start = time.perf_counter()
    pending_writes: list[dict] = []
    write_lock = threading.Lock()

    tagged_count = 0
    failed_count = 0
    written_count = 0
    completed_batches = 0
    processed_questions = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_batch = {executor.submit(_tag_batch, batch): batch for batch in batches}

        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                batch_tags = future.result()
            except Exception as exc:
                logging.warning("Batch execution crashed: %s", exc)
                batch_tags = [_default_tag(row) for row in batch]

            batch_success = 0
            for tag in batch_tags:
                try:
                    confidence = float(tag.get("tag_confidence", 0.0))
                except Exception:
                    confidence = 0.0
                if confidence > 0.0:
                    batch_success += 1

            batch_failed = len(batch_tags) - batch_success
            tagged_count += batch_success
            failed_count += batch_failed
            completed_batches += 1
            processed_questions += len(batch_tags)

            write_payload: list[dict] = []
            with write_lock:
                pending_writes.extend(batch_tags)
                if len(pending_writes) >= COMMIT_EVERY or completed_batches == batch_count:
                    write_payload = pending_writes[:]
                    pending_writes.clear()

            if write_payload:
                written_count += _write_tags(db_path, write_payload)

            if completed_batches % 5 == 0 or completed_batches == batch_count:
                elapsed = max(time.perf_counter() - start, 1e-9)
                rate = processed_questions / elapsed
                remaining = max(0, total_questions - processed_questions)
                eta_seconds = remaining / rate if rate > 0 else float("inf")
                logging.info(
                    "Progress: batches %s/%s | tagged=%s failed=%s | rate=%.2f q/s | ETA=%.2f seconds",
                    completed_batches,
                    batch_count,
                    tagged_count,
                    failed_count,
                    rate,
                    eta_seconds,
                )

    total_minutes = (time.perf_counter() - start) / 60.0
    logging.info(
        "Completed fast retag in %.2f minutes | tagged=%s failed=%s written=%s",
        total_minutes,
        tagged_count,
        failed_count,
        written_count,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast batch NEET retagger")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file")
    parser.add_argument("--workers", type=int, default=8, help="Worker thread count")
    parser.add_argument("--limit", type=int, default=None, help="Optional max rows to process")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
    args = _parse_args()
    run(db_path=str(args.db), workers=int(args.workers), limit=args.limit)


if __name__ == "__main__":
    main()
