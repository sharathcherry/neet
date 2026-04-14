from __future__ import annotations

import argparse
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import sqlite3
from dotenv import load_dotenv
from groq import Groq
from requests import Session

QUALITY_GROQ_MODEL_NAME = "llama-3.3-70b-versatile"
SPEED_GROQ_MODEL_NAME = "llama-3.1-8b-instant"
NVIDIA_MODEL_NAME = "meta/llama-3.1-8b-instruct"
NVIDIA_CHAT_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
REQUIRED_KEYS = {"topic", "subtopic", "difficulty", "bloom_level", "subject", "tag_confidence"}
ALLOWED_DIFFICULTY = {"easy", "medium", "hard"}
ALLOWED_BLOOM = {"remember", "understand", "apply", "analyze", "evaluate", "create"}
ALLOWED_SUBJECTS = {"Physics", "Chemistry", "Botany", "Zoology"}
DEFAULT_PROVIDER = "groq"
PROFILE_CHOICES = ("quality", "balanced", "speed")
_thread_local = threading.local()


@dataclass(frozen=True)
class RuntimeConfig:
    max_tag_attempts: int
    max_tokens: int
    request_timeout_seconds: int
    cooldown_seconds: float
    write_batch_size: int


def _split_keys(raw: str | None) -> list[str]:
    if not raw:
        return []
    parts = re.split(r"[\n,;]+", raw)
    return [part.strip() for part in parts if part.strip()]


def _dedupe_keys(keys: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for key in keys:
        cleaned = str(key or "").strip()
        if cleaned and cleaned not in seen:
            ordered.append(cleaned)
            seen.add(cleaned)
    return ordered


def build_runtime_config(profile: str, args: argparse.Namespace) -> RuntimeConfig:
    profile_name = str(profile or "quality").strip().lower()
    if profile_name == "speed":
        defaults = RuntimeConfig(
            max_tag_attempts=2,
            max_tokens=240,
            request_timeout_seconds=45,
            cooldown_seconds=0.0,
            write_batch_size=250,
        )
    elif profile_name == "balanced":
        defaults = RuntimeConfig(
            max_tag_attempts=3,
            max_tokens=420,
            request_timeout_seconds=75,
            cooldown_seconds=0.03,
            write_batch_size=150,
        )
    else:
        defaults = RuntimeConfig(
            max_tag_attempts=5,
            max_tokens=320,
            request_timeout_seconds=120,
            cooldown_seconds=0.05,
            write_batch_size=200,
        )

    max_attempts = int(args.max_attempts) if args.max_attempts is not None else defaults.max_tag_attempts
    max_tokens = int(args.max_tokens) if args.max_tokens is not None else defaults.max_tokens
    request_timeout_seconds = (
        int(args.request_timeout) if args.request_timeout is not None else defaults.request_timeout_seconds
    )
    cooldown_seconds = (
        float(args.cooldown_ms) / 1000.0 if args.cooldown_ms is not None else defaults.cooldown_seconds
    )
    write_batch_size = int(args.write_batch) if args.write_batch is not None else defaults.write_batch_size

    return RuntimeConfig(
        max_tag_attempts=max(1, max_attempts),
        max_tokens=max(80, max_tokens),
        request_timeout_seconds=max(10, request_timeout_seconds),
        cooldown_seconds=max(0.0, cooldown_seconds),
        write_batch_size=max(20, write_batch_size),
    )


def resolve_models(profile: str, args: argparse.Namespace) -> tuple[str, str]:
    profile_name = str(profile or "quality").strip().lower()

    if profile_name == "speed":
        default_groq_model = SPEED_GROQ_MODEL_NAME
    else:
        default_groq_model = QUALITY_GROQ_MODEL_NAME

    groq_model = str(args.groq_model or "").strip() or default_groq_model
    nvidia_model = str(args.nvidia_model or "").strip() or NVIDIA_MODEL_NAME
    return groq_model, nvidia_model


def resolve_worker_count(requested: int, provider: str, profile: str, groq_key_count: int) -> int:
    if int(requested) > 0:
        return max(1, int(requested))

    profile_name = str(profile or "quality").strip().lower()
    provider_name = str(provider or "groq").strip().lower()

    if provider_name != "groq":
        return 12 if profile_name == "speed" else 8

    key_count = max(1, int(groq_key_count))
    if profile_name == "speed":
        return min(64, max(8, key_count * 8))
    if profile_name == "balanced":
        return min(48, max(4, key_count * 4))

    # Quality profile favors reliability. One key on 70B should stay conservative.
    if key_count == 1:
        return 1
    return min(24, max(4, key_count * 3))


def acquire_process_lock(lock_path: Path) -> int:
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(
            f"Another retag process appears to be running (lock file: {lock_path}). "
            "Stop the existing job or rerun with --force-lock if the lock is stale."
        ) from exc

    os.write(fd, str(os.getpid()).encode("utf-8"))
    return fd


def release_process_lock(lock_fd: int | None, lock_path: Path) -> None:
    if lock_fd is None:
        return
    try:
        os.close(lock_fd)
    except OSError:
        pass
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass


class GroqClientPool:
    """Round-robin pool of Groq clients for multi-account tagging."""

    def __init__(self, api_keys: list[str], model_name: str) -> None:
        if not api_keys:
            raise ValueError("At least one Groq API key is required")
        self._clients = [Groq(api_key=key) for key in api_keys]
        self._model_name = model_name
        self._lock = threading.Lock()
        self._index = 0

    @property
    def size(self) -> int:
        return len(self._clients)

    def next_client(self) -> Groq:
        with self._lock:
            client = self._clients[self._index % len(self._clients)]
            self._index += 1
            return client

    @property
    def model_name(self) -> str:
        return self._model_name


class RateLimitError(RuntimeError):
    """Raised when NVIDIA API returns HTTP 429 with optional retry-after hint."""

    def __init__(self, wait_seconds: float) -> None:
        super().__init__(f"rate_limited:{wait_seconds}")
        self.wait_seconds = max(1.0, float(wait_seconds))


SYSTEM_PROMPT = """You are an expert classifier for NEET / AIIMS (Indian medical entrance) questions.
These questions cover Physics, Chemistry, Botany, and Zoology at NCERT Class 11-12 level.

Respond with ONLY a valid JSON object - no markdown fences, no explanation text.

Required keys:
  "topic"          - NCERT chapter name (e.g. "Laws of Motion", "Cell: The Unit of Life")
  "subtopic"       - specific concept within the topic
  "difficulty"     - "easy" | "medium" | "hard"
  "bloom_level"    - "remember" | "understand" | "apply" | "analyze" | "evaluate" | "create"
  "subject"        - "Physics" | "Chemistry" | "Botany" | "Zoology"
  "tag_confidence" - float 0.0-1.0

Choose the safest classification you can justify from the question text.
If the question is ambiguous, prefer a slightly lower confidence over guessing.

Difficulty:
  easy   = direct NCERT recall, one fact, definition
  medium = formula use, 2-step reasoning, concept application
  hard   = multi-step, cross-concept, novel scenario, numerical derivation

Bloom level primary verb:
  remember=recall/state/identify, understand=explain/describe,
  apply=calculate/solve, analyze=compare/differentiate,
  evaluate=justify/assess, create=derive/design

Return ONLY valid JSON. No other text."""


def default_tags() -> dict[str, Any]:
    return {
        "topic": "unknown",
        "subtopic": "unknown",
        "difficulty": "unknown",
        "bloom_level": "unknown",
        "subject": "Unknown",
        "tag_confidence": 0.0,
    }


def normalize_subject(value: Any) -> str:
    normalized = str(value or "Unknown").strip().title()
    return normalized if normalized in ALLOWED_SUBJECTS else "Unknown"


def parse_options(raw_options: str | None) -> list[str]:
    if not raw_options:
        return []
    try:
        data = json.loads(raw_options)
        if not isinstance(data, list):
            return []
        return [str(item).strip() for item in data if str(item).strip()]
    except Exception:
        return []


def build_user_prompt(question: dict[str, Any]) -> str:
    lines = [
        f"Source year: {question['source_year']}",
        f"Question type: {question['question_type']}",
        "",
        "Question:",
        str(question["question_text"])[:420],
    ]

    options = question.get("options", [])
    if options:
        lines.append("")
        lines.append("Options:")
        for idx, option in enumerate(options[:4], start=1):
            lines.append(f"  ({idx}) {str(option)[:180]}")

    return "\n".join(lines)


def strip_fences(text: str) -> str:
    cleaned = text.strip()
    cleaned = cleaned.removeprefix("```json").removeprefix("```").removesuffix("```")
    return cleaned.strip()


def parse_json_payload(text: str) -> dict[str, Any]:
    cleaned = strip_fences(text)
    try:
        payload = json.loads(cleaned)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass

    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        raise ValueError("No JSON object detected in model response")

    payload = json.loads(match.group(0))
    if not isinstance(payload, dict):
        raise ValueError("Parsed JSON is not an object")
    return payload


def sanitize_tags(payload: dict[str, Any]) -> dict[str, Any]:
    if not REQUIRED_KEYS.issubset(set(payload.keys())):
        missing = REQUIRED_KEYS.difference(set(payload.keys()))
        raise ValueError(f"Missing required keys: {sorted(missing)}")

    difficulty = str(payload.get("difficulty", "unknown")).strip().lower()
    if difficulty not in ALLOWED_DIFFICULTY:
        difficulty = "unknown"

    bloom = str(payload.get("bloom_level", "unknown")).strip().lower()
    if bloom not in ALLOWED_BLOOM:
        bloom = "unknown"

    confidence = payload.get("tag_confidence", 0.0)
    try:
        confidence = float(confidence)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    return {
        "topic": str(payload.get("topic", "unknown")).strip() or "unknown",
        "subtopic": str(payload.get("subtopic", "unknown")).strip() or "unknown",
        "difficulty": difficulty,
        "bloom_level": bloom,
        "subject": normalize_subject(payload.get("subject", "Unknown")),
        "tag_confidence": confidence,
    }


def get_thread_session() -> Session:
    session = getattr(_thread_local, "http_session", None)
    if session is None:
        session = requests.Session()
        _thread_local.http_session = session
    return session


def _tag_with_groq(
    question: dict[str, Any],
    client: Groq | None,
    model_name: str,
    runtime: RuntimeConfig,
    pool: GroqClientPool | None = None,
) -> dict[str, Any]:
    prompt = build_user_prompt(question)
    use_json_response_format = True

    for attempt in range(runtime.max_tag_attempts):
        try:
            request_client = pool.next_client() if pool is not None else client
            if request_client is None:
                raise RuntimeError("Groq client is not available")
            request_payload: dict[str, Any] = {
                "model": model_name,
                "temperature": 0.0,
                "top_p": 1.0,
                "max_tokens": runtime.max_tokens,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
            }
            if use_json_response_format:
                request_payload["response_format"] = {"type": "json_object"}

            response = request_client.chat.completions.create(**request_payload)
            text = str((response.choices[0].message.content if response.choices else "") or "")
            payload = parse_json_payload(text)
            return sanitize_tags(payload)
        except Exception as exc:
            message = str(exc).lower()
            if use_json_response_format and "response_format" in message and (
                "unsupported" in message or "invalid" in message
            ):
                use_json_response_format = False
                continue
            if looks_like_rate_limit(exc):
                time.sleep(min(10.0, 1.0 + attempt * 1.2))
            elif attempt < (runtime.max_tag_attempts - 1):
                time.sleep(min(3.0, 0.6 + attempt * 0.5))
        finally:
            if runtime.cooldown_seconds > 0:
                time.sleep(runtime.cooldown_seconds)

    return default_tags()


def _tag_with_nvidia(question: dict[str, Any], api_key: str, model_name: str, runtime: RuntimeConfig) -> dict[str, Any]:
    prompt = build_user_prompt(question)

    for attempt in range(runtime.max_tag_attempts):
        try:
            session = get_thread_session()
            response = session.post(
                NVIDIA_CHAT_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model_name,
                    "temperature": 0.0,
                    "top_p": 1.0,
                    "max_tokens": runtime.max_tokens,
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "neet_question_tags",
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "topic": {"type": "string"},
                                    "subtopic": {"type": "string"},
                                    "difficulty": {"type": "string", "enum": ["easy", "medium", "hard"]},
                                    "bloom_level": {
                                        "type": "string",
                                        "enum": ["remember", "understand", "apply", "analyze", "evaluate", "create"],
                                    },
                                    "subject": {
                                        "type": "string",
                                        "enum": ["Physics", "Chemistry", "Botany", "Zoology"],
                                    },
                                    "tag_confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                                },
                                "required": [
                                    "topic",
                                    "subtopic",
                                    "difficulty",
                                    "bloom_level",
                                    "subject",
                                    "tag_confidence",
                                ],
                                "additionalProperties": False,
                            },
                        },
                    },
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                },
                timeout=runtime.request_timeout_seconds,
            )
            if response.status_code == 429:
                retry_after_header = response.headers.get("Retry-After")
                retry_after = 8.0
                if retry_after_header:
                    try:
                        retry_after = float(retry_after_header)
                    except Exception:
                        retry_after = 8.0
                raise RateLimitError(retry_after)

            response.raise_for_status()
            payload_root = response.json()
            choices = payload_root.get("choices", [])
            text = ""
            if choices:
                text = str(((choices[0] or {}).get("message", {}) or {}).get("content", "") or "")
            payload = parse_json_payload(text)
            return sanitize_tags(payload)
        except RateLimitError as exc:
            if attempt < (runtime.max_tag_attempts - 1):
                time.sleep(max(0.8, min(4.0, exc.wait_seconds), float((attempt + 1) * 0.8)))
        except Exception:
            if attempt < (runtime.max_tag_attempts - 1):
                time.sleep(min(0.8, 0.2 + attempt * 0.2))
        finally:
            if runtime.cooldown_seconds > 0:
                time.sleep(runtime.cooldown_seconds)

    return default_tags()


def looks_like_rate_limit(exc: Exception) -> bool:
    message = str(exc).lower()
    return "429" in message or "rate limit" in message or "too many requests" in message


def tag_one(
    question: dict[str, Any],
    provider: str,
    groq_client: Groq | None,
    groq_pool: GroqClientPool | None,
    nvidia_api_key: str | None,
    groq_model: str,
    nvidia_model: str,
    runtime: RuntimeConfig,
) -> tuple[int, dict[str, Any], bool]:
    provider_name = str(provider).strip().lower()
    if provider_name == "groq":
        if groq_client is None and groq_pool is None:
            raise RuntimeError("Groq client is not available")
        tags = _tag_with_groq(question, groq_client, groq_model, runtime, groq_pool)
    else:
        if not nvidia_api_key:
            raise RuntimeError("NVIDIA API key is not available")
        tags = _tag_with_nvidia(question, nvidia_api_key, nvidia_model, runtime)

    ok = float(tags.get("tag_confidence", 0.0)) > 0.0
    return int(question["id"]), tags, ok


def fetch_pending_questions(conn: sqlite3.Connection, limit: int | None) -> list[dict[str, Any]]:
    query = (
        "SELECT id, question_text, options, question_type, source_year "
        "FROM questions WHERE COALESCE(tag_confidence, 0.0) <= 0.0 ORDER BY id"
    )
    params: list[Any] = []
    if limit is not None and limit > 0:
        query += " LIMIT ?"
        params.append(int(limit))

    rows = conn.execute(query, params).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "question_text": str(row["question_text"] or ""),
                "question_type": str(row["question_type"] or "unknown"),
                "source_year": int(row["source_year"] or 0),
                "options": parse_options(row["options"]),
            }
        )
    return result


def write_updates(conn: sqlite3.Connection, updates: list[tuple[Any, ...]]) -> None:
    if not updates:
        return
    conn.executemany(
        """
        UPDATE questions
        SET topic=?, subtopic=?, difficulty=?, bloom_level=?, subject=?, tag_confidence=?
        WHERE id=?
        """,
        updates,
    )
    conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retag untagged question rows using Groq or NVIDIA")
    parser.add_argument("--db", default="data/db/questions.db", help="Path to SQLite database")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for test runs")
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Parallel worker threads (0 = auto based on provider/profile/key count)",
    )
    parser.add_argument(
        "--profile",
        choices=list(PROFILE_CHOICES),
        default="quality",
        help="Runtime profile: quality keeps strongest quality defaults, balanced/speed trade quality for throughput.",
    )
    parser.add_argument(
        "--provider",
        choices=["groq", "nvidia"],
        default=DEFAULT_PROVIDER,
        help="Tagging backend to use. Groq is the quality-first default.",
    )
    parser.add_argument(
        "--groq-key",
        action="append",
        default=[],
        help="Additional Groq API key. Repeat this flag for multiple accounts.",
    )
    parser.add_argument(
        "--groq-keys",
        default="",
        help="Comma, semicolon, or newline separated Groq API keys.",
    )
    parser.add_argument(
        "--groq-model",
        default="",
        help="Groq model name override. Empty uses profile default (quality -> llama-3.3-70b-versatile).",
    )
    parser.add_argument("--nvidia-model", default="", help="NVIDIA model name override")
    parser.add_argument("--max-attempts", type=int, default=None, help="Override retry attempts per question")
    parser.add_argument("--max-tokens", type=int, default=None, help="Override max tokens per model call")
    parser.add_argument("--request-timeout", type=int, default=None, help="Override request timeout (seconds)")
    parser.add_argument("--cooldown-ms", type=int, default=None, help="Override per-call cooldown in milliseconds")
    parser.add_argument("--write-batch", type=int, default=None, help="Override SQLite batch update size")
    parser.add_argument("--progress-every", type=int, default=50, help="Progress log interval")
    parser.add_argument(
        "--lock-file",
        default=".retag.lock",
        help="Process lock file path to prevent duplicate retag runs on the same DB",
    )
    parser.add_argument(
        "--force-lock",
        action="store_true",
        help="Remove an existing lock file before starting (only if you are sure lock is stale)",
    )
    return parser.parse_args()


def resolve_groq_api_key() -> str:
    load_dotenv()
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not found in environment or .env")
    return api_key


def resolve_groq_api_keys(explicit_keys: list[str], extra_keys: str) -> list[str]:
    load_dotenv()
    keys: list[str] = []
    keys.extend(explicit_keys)
    keys.extend(_split_keys(extra_keys))
    keys.extend(_split_keys(os.environ.get("GROQ_API_KEYS", "")))

    single_key = os.environ.get("GROQ_API_KEY", "").strip()
    if single_key:
        keys.append(single_key)

    unique_keys = _dedupe_keys(keys)
    if not unique_keys:
        raise RuntimeError("No Groq API keys found in arguments or environment")
    return unique_keys


def resolve_nvidia_api_key() -> str:
    load_dotenv()
    api_key = os.environ.get("NVIDIA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("NVIDIA_API_KEY not found in environment or .env")
    return api_key


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("groq").setLevel(logging.WARNING)

    profile = str(args.profile).strip().lower()
    provider = str(args.provider).strip().lower()
    runtime = build_runtime_config(profile, args)
    groq_model, nvidia_model = resolve_models(profile, args)

    groq_client: Groq | None = None
    groq_pool: GroqClientPool | None = None
    nvidia_api_key: str | None = None
    groq_keys: list[str] = []

    lock_path = Path(str(args.lock_file)).resolve()
    lock_fd: int | None = None

    if args.force_lock and lock_path.exists():
        try:
            lock_path.unlink()
        except OSError as exc:
            raise RuntimeError(f"Failed to remove lock file {lock_path}: {exc}") from exc

    lock_fd = acquire_process_lock(lock_path)

    try:
        if provider == "groq":
            groq_keys = resolve_groq_api_keys(list(args.groq_key or []), str(args.groq_keys or ""))
            if len(groq_keys) == 1:
                groq_client = Groq(api_key=groq_keys[0])
            else:
                groq_pool = GroqClientPool(groq_keys, groq_model)
                groq_client = groq_pool.next_client()
        else:
            nvidia_api_key = resolve_nvidia_api_key()

        workers = resolve_worker_count(int(args.workers), provider, profile, len(groq_keys))

        conn = sqlite3.connect(str(Path(args.db)))
        conn.row_factory = sqlite3.Row

        # Faster transactional throughput without changing tagging quality semantics.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")

        try:
            pending = fetch_pending_questions(conn, args.limit)
            total = len(pending)
            if total == 0:
                print("No pending rows to retag.")
                return

            if profile == "quality":
                est_seconds_each = 1.6
            elif profile == "balanced":
                est_seconds_each = 0.9
            else:
                est_seconds_each = 0.45
            rough_minutes = max(1, int(round((total * est_seconds_each) / max(1, workers) / 60 + 0.5)))

            provider_label = "Groq" if provider == "groq" else "NVIDIA"
            key_count = len(groq_keys) if provider == "groq" else 1
            print(
                f"Tagging {total} questions using {provider_label} | profile={profile} | "
                f"workers={workers} | keys={key_count}"
            )
            print(
                f"Model: {(groq_model if provider == 'groq' else nvidia_model)} | retries={runtime.max_tag_attempts} | "
                f"cooldown={runtime.cooldown_seconds:.3f}s | write_batch={runtime.write_batch_size}"
            )
            print(f"Estimated time: ~{rough_minutes} minutes")

            processed = 0
            tagged = 0
            failed = 0
            progress_every = max(1, int(args.progress_every))
            pending_updates: list[tuple[Any, ...]] = []

            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(
                        tag_one,
                        item,
                        provider,
                        groq_client,
                        groq_pool,
                        nvidia_api_key,
                        groq_model,
                        nvidia_model,
                        runtime,
                    ): item["id"]
                    for item in pending
                }

                for future in as_completed(future_map):
                    try:
                        row_id, tags, ok = future.result()
                    except Exception as exc:
                        row_id = int(future_map[future])
                        tags = default_tags()
                        ok = False
                        logging.warning("Tagging failed for row_id=%s: %s", row_id, exc)

                    pending_updates.append(
                        (
                            tags["topic"],
                            tags["subtopic"],
                            tags["difficulty"],
                            tags["bloom_level"],
                            tags["subject"],
                            tags["tag_confidence"],
                            row_id,
                        )
                    )

                    processed += 1
                    if ok and float(tags.get("tag_confidence", 0.0)) > 0.0:
                        tagged += 1
                    else:
                        failed += 1

                    if len(pending_updates) >= runtime.write_batch_size:
                        write_updates(conn, pending_updates)
                        pending_updates.clear()

                    if processed % progress_every == 0 or processed == total:
                        logging.info("Progress: %s/%s  |  tagged=%s  failed=%s", processed, total, tagged, failed)

            if pending_updates:
                write_updates(conn, pending_updates)

            print("\n── DONE ───────────────────────────────────────────")
            print(f"  Total processed:     {processed}")
            print(f"  Successfully tagged: {tagged}")
            print(f"  Failed (unknown):    {failed}")
            print("\n  Run tools/audit_tags.py to see tag distribution.")
        finally:
            conn.close()
    finally:
        release_process_lock(lock_fd, lock_path)


if __name__ == "__main__":
    main()
