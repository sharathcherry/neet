from __future__ import annotations

import argparse
import re
from typing import Any

from pipeline.extractor import PAGE_BREAK_SENTINEL, extract_text

QUESTION_PATTERNS = [
    re.compile(r"^\s*Q\.?\s*(\d+)\b", re.IGNORECASE),
    re.compile(r"^\s*(\d+)[\.)]\s+"),
    re.compile(r"^\s*Question\s+(\d+)\b", re.IGNORECASE),
]

OPTION_PATTERN = re.compile(r"^\s*(?:\(?([A-D])\)?[\.)])\s*(.+)$", re.IGNORECASE)


def _is_caps_heading(line: str) -> bool:
    letters = [char for char in line if char.isalpha()]
    if len(letters) < 6:
        return False
    return sum(1 for char in letters if char.isupper()) / len(letters) > 0.85


def _extract_question_number(line: str, fallback_number: int) -> int:
    for pattern in QUESTION_PATTERNS:
        match = pattern.search(line)
        if match:
            return int(match.group(1))
    return fallback_number


def _is_question_start(line: str, next_lines: list[str]) -> bool:
    if any(pattern.search(line) for pattern in QUESTION_PATTERNS):
        return True
    if _is_caps_heading(line):
        option_hits = sum(1 for item in next_lines[:6] if OPTION_PATTERN.search(item))
        return option_hits >= 2
    return False


def _detect_question_type(question_text: str, options: list[str]) -> str:
    if len(options) == 4:
        return "mcq"

    lower = question_text.lower()
    short_keywords = ("state", "write", "define", "list")
    long_keywords = ("explain", "describe", "discuss", "evaluate")

    if "?" in question_text or any(keyword in lower for keyword in short_keywords):
        return "short_answer"
    if any(keyword in lower for keyword in long_keywords):
        return "long_answer"
    return "unknown"


def _split_page_questions(page_text: str, page_hint: int) -> list[dict[str, Any]]:
    lines = [line.strip() for line in page_text.splitlines() if line.strip()]
    questions: list[dict[str, Any]] = []

    start_index = None
    fallback = 1
    for index, line in enumerate(lines):
        next_lines = lines[index + 1 : index + 8]
        if _is_question_start(line, next_lines):
            if start_index is not None:
                chunk = lines[start_index:index]
                if chunk:
                    questions.append({"chunk": chunk, "page_hint": page_hint, "fallback": fallback})
                    fallback += 1
            start_index = index

    if start_index is not None:
        chunk = lines[start_index:]
        if chunk:
            questions.append({"chunk": chunk, "page_hint": page_hint, "fallback": fallback})

    return questions


def split_questions(raw_text: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Split extracted text into structured question objects using regex heuristics."""
    if not raw_text or not raw_text.strip():
        raise ValueError("No text provided to split_questions; raw_text is empty.")

    pages = [page for page in raw_text.split(PAGE_BREAK_SENTINEL) if page.strip()]
    staged_chunks: list[dict[str, Any]] = []

    for page_index, page_text in enumerate(pages, start=1):
        staged_chunks.extend(_split_page_questions(page_text, page_index))

    if len(staged_chunks) < 2:
        raise ValueError("Question detection failed: fewer than 2 questions were detected.")

    source_year = int(metadata.get("year") or metadata.get("source_year") or 0)
    source_pdf = str(metadata.get("source_pdf") or metadata.get("pdf_path") or "")

    output: list[dict[str, Any]] = []
    for item in staged_chunks:
        chunk_lines = item["chunk"]
        first_line = chunk_lines[0]
        question_number = _extract_question_number(first_line, item["fallback"])

        options: list[str] = []
        stem_lines: list[str] = []
        for line in chunk_lines:
            option_match = OPTION_PATTERN.search(line)
            if option_match and len(options) < 4:
                options.append(option_match.group(2).strip())
            else:
                stem_lines.append(line)

        question_text = "\n".join(stem_lines).strip()
        output.append(
            {
                "question_number": question_number,
                "question_text": question_text,
                "options": options if len(options) == 4 else [],
                "question_type": _detect_question_type(question_text, options),
                "source_year": source_year,
                "source_pdf": source_pdf,
                "page_hint": item["page_hint"],
            }
        )

    return output


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Split questions from a PDF using regex heuristics.")
    parser.add_argument("pdf_path", help="Path to a source PDF")
    return parser


def main() -> None:
    """Run extraction + splitting and print the first parsed question."""
    args = _build_cli_parser().parse_args()

    extract_result = extract_text(args.pdf_path)
    if extract_result.get("error"):
        print(f"ERROR: {extract_result['error']}")
        return

    metadata = {"source_year": 0, "source_pdf": args.pdf_path}
    questions = split_questions(extract_result.get("raw_text", ""), metadata)
    print(f"Questions detected: {len(questions)}")
    print(questions[0] if questions else {})


if __name__ == "__main__":
    main()
