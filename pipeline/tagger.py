from __future__ import annotations

import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from groq import Groq
from dotenv import load_dotenv

MODEL_NAME = "llama-3.3-70b-versatile"
ALLOWED_DIFFICULTY = {"easy", "medium", "hard"}
ALLOWED_BLOOM = {"remember", "understand", "apply", "analyze", "evaluate", "create"}
ALLOWED_SUBJECTS = {"Physics", "Chemistry", "Botany", "Zoology"}
REQUIRED_KEYS = {"topic", "subtopic", "difficulty", "bloom_level", "subject", "tag_confidence"}

SYSTEM_PROMPT = (
    "You are an expert academic question classifier for Indian competitive exams (NEET, JEE, CBSE).\n"
    "Given a question, respond with ONLY a JSON object - no preamble, no markdown, no explanation.\n"
    "The JSON must have exactly these keys:\n"
    "  topic, subtopic, difficulty, bloom_level, subject, tag_confidence\n"
    "Use the most specific NCERT-style topic and subtopic you can justify from the question.\n"
    "If the question is ambiguous, choose the safest classification and lower confidence rather than guessing.\n"
    "Difficulty rules: easy = recall/single-step; medium = 2-3 steps or application; hard = multi-step/novel.\n"
    "Bloom level rules: match the primary cognitive verb in the question.\n"
    "tag_confidence: your confidence in this classification as a float between 0.0 and 1.0.\n"
    "Confidence guide: 0.9+ only when the topic is very clear; 0.6-0.8 when likely; below 0.6 when uncertain."
)


def _unknown_fields() -> dict[str, Any]:
    return {
        "topic": "unknown",
        "subtopic": "unknown",
        "difficulty": "unknown",
        "bloom_level": "unknown",
        "subject": "Unknown",
        "tag_confidence": 0.0,
    }


def _extract_text_response(response: Any) -> str:
    choices = getattr(response, "choices", [])
    if not choices:
        return ""
    message = getattr(choices[0], "message", None)
    if message is None:
        return ""
    return str(getattr(message, "content", "") or "").strip()


def _parse_json_payload(text: str) -> dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("No JSON object detected in model response")
    return json.loads(match.group(0))


def _normalize_subject(subject: Any) -> str:
    normalized = str(subject or "Unknown").strip().title()
    return normalized if normalized in ALLOWED_SUBJECTS else "Unknown"


def _sanitize_tags(payload: dict[str, Any]) -> dict[str, Any]:
    if not REQUIRED_KEYS.issubset(set(payload.keys())):
        missing = REQUIRED_KEYS.difference(set(payload.keys()))
        raise ValueError(f"Missing required keys: {sorted(missing)}")

    fields = _unknown_fields()
    for key in fields:
        if key in payload:
            fields[key] = payload[key]

    difficulty = str(fields.get("difficulty", "unknown")).strip().lower()
    if difficulty not in ALLOWED_DIFFICULTY:
        difficulty = "unknown"
    fields["difficulty"] = difficulty

    bloom = str(fields.get("bloom_level", "unknown")).strip().lower()
    if bloom not in ALLOWED_BLOOM:
        bloom = "unknown"
    fields["bloom_level"] = bloom

    fields["subject"] = _normalize_subject(fields.get("subject", "Unknown"))

    try:
        fields["tag_confidence"] = float(fields.get("tag_confidence", 0.0))
    except Exception:
        fields["tag_confidence"] = 0.0

    fields["tag_confidence"] = max(0.0, min(1.0, fields["tag_confidence"]))
    return fields


def tag_question(question: dict[str, Any], client: Groq) -> dict[str, Any]:
    """Tag one question using Groq and return the enriched question dictionary."""
    user_prompt = json.dumps(
        {
            "question_text": question.get("question_text", ""),
            "options": question.get("options", []),
            "question_type": question.get("question_type", "unknown"),
            "source_year": question.get("source_year", 0),
        },
        ensure_ascii=False,
    )

    for attempt in range(2):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                temperature=0.0,
                top_p=1.0,
                max_tokens=1024,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )
            payload = _parse_json_payload(_extract_text_response(response))
            if not REQUIRED_KEYS.issubset(set(payload.keys())):
                raise ValueError(f"Missing required keys: {sorted(REQUIRED_KEYS.difference(set(payload.keys())))}")
            tagged = dict(question)
            tagged.update(_sanitize_tags(payload))
            return tagged
        except Exception as exc:
            if attempt == 1:
                logging.warning("Tagging failed after retry: %s", exc)
            time.sleep(0.1)
        finally:
            time.sleep(0.1)

    tagged = dict(question)
    tagged.update(_unknown_fields())
    return tagged


def _tag_with_index(index: int, question: dict[str, Any], client: Groq) -> tuple[int, dict[str, Any]]:
    return index, tag_question(question, client)


def batch_tag(
    questions: list[dict[str, Any]],
    client: Groq,
    max_workers: int = 5,
) -> list[dict[str, Any]]:
    """Tag a list of questions concurrently while preserving original order."""
    if not questions:
        return []

    max_workers = max(1, int(max_workers))
    results: list[dict[str, Any]] = [dict(item) for item in questions]
    total = len(questions)
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_tag_with_index, idx, q, client) for idx, q in enumerate(questions)]
        for future in as_completed(futures):
            idx, tagged = future.result()
            results[idx] = tagged
            completed += 1
            if completed % 10 == 0 or completed == total:
                logging.info("Tagged %s / %s questions", completed, total)

    return results


def build_client() -> Groq:
    """Create a Groq client using GROQ_API_KEY from environment."""
    load_dotenv()
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set")
    return Groq(api_key=api_key)
