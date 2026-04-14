from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from pipeline.extractor import extract_text
from pipeline.ingestor import init_db, insert_questions_batch
from pipeline.splitter import split_questions
from pipeline.tagger import batch_tag, build_client


def _load_manifest(manifest_path: Path) -> list[dict[str, Any]]:
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("manifest.json must contain a list")
        return payload

    fallback = Path("data/neet_papers/manifest.json")
    if fallback.exists():
        payload = json.loads(fallback.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return payload

    raise FileNotFoundError(f"Manifest not found: {manifest_path}")


def _normalize_entry(entry: dict[str, Any]) -> dict[str, Any]:
    year = int(entry.get("year", 0) or 0)
    pdf_path = str(entry.get("pdf_path") or entry.get("file_path") or "")
    source_url = str(entry.get("source_url") or "")
    paper_type = str(entry.get("paper_type") or "question_paper")
    return {
        "year": year,
        "pdf_path": pdf_path,
        "source_url": source_url,
        "paper_type": paper_type,
    }


def _resolve_pdf_path(pdf_path: str) -> Path:
    candidate = Path(pdf_path)
    if candidate.exists():
        return candidate

    prefixed = Path("data") / pdf_path
    if prefixed.exists():
        return prefixed

    fallback = Path("data/neet_papers/papers") / Path(pdf_path).name
    if fallback.exists():
        return fallback

    return candidate


def _unknown_tags(question: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(question)
    enriched.update(
        {
            "topic": "unknown",
            "subtopic": "unknown",
            "difficulty": "unknown",
            "bloom_level": "unknown",
            "subject": "Unknown",
            "tag_confidence": 0.0,
        }
    )
    return enriched


def run_pipeline(
    manifest_path: Path,
    db_path: Path,
    dry_run: bool,
    year: int | None,
    workers: int,
    progress_hook: Any | None = None,
) -> dict[str, int]:
    """Run extraction, splitting, tagging and ingestion for manifest entries."""
    entries = [_normalize_entry(item) for item in _load_manifest(manifest_path)]
    if year is not None:
        entries = [item for item in entries if int(item.get("year", 0)) == int(year)]
    expected_total = len(entries)

    client = None
    if not dry_run:
        try:
            client = build_client()
        except Exception as exc:
            logging.warning(
                "Groq client unavailable; continuing with unknown tags. reason=%s",
                exc,
            )

    total_pdfs = 0
    total_questions = 0
    total_inserted = 0

    conn = init_db(str(db_path))
    try:
        for index, entry in enumerate(entries, start=1):
            try:
                pdf_file = _resolve_pdf_path(entry["pdf_path"])
                total_pdfs += 1

                extracted = extract_text(str(pdf_file))
                if extracted.get("error"):
                    logging.error("PDF extraction failed for %s: %s", pdf_file, extracted["error"])
                    continue

                metadata = {
                    "source_year": entry["year"],
                    "source_pdf": str(pdf_file),
                    "source_url": entry["source_url"],
                    "paper_type": entry["paper_type"],
                }

                try:
                    questions = split_questions(extracted.get("raw_text", ""), metadata)
                except Exception as exc:
                    logging.error("Question splitting failed for %s: %s", pdf_file, exc)
                    continue

                total_questions += len(questions)
                inserted_count = 0

                if not dry_run:
                    if client is None:
                        tagged = [_unknown_tags(item) for item in questions]
                    else:
                        try:
                            tagged = batch_tag(questions, client=client, max_workers=workers)
                        except Exception as exc:
                            logging.error("Batch tagging failed for %s: %s", pdf_file, exc)
                            tagged = [_unknown_tags(item) for item in questions]

                    inserted_count = insert_questions_batch(conn, tagged)
                    total_inserted += inserted_count

                logging.info(
                    "Year %s: extracted %s pages, %s questions, %s inserted",
                    entry["year"],
                    extracted.get("page_count", 0),
                    len(questions),
                    inserted_count,
                )
            finally:
                if progress_hook is not None:
                    progress_hook(index, expected_total)
    finally:
        conn.close()

    return {
        "total_pdfs": total_pdfs,
        "total_questions": total_questions,
        "total_inserted": total_inserted,
        "total_skipped_dupes": max(0, total_questions - total_inserted),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run end-to-end question paper pipeline.")
    parser.add_argument("--manifest", type=Path, default=Path("data/manifest.json"))
    parser.add_argument("--db", type=Path, default=Path("data/db/questions.db"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--workers", type=int, default=5)
    return parser


def _print_summary(summary: dict[str, int]) -> None:
    print("\nFINAL SUMMARY")
    print("=" * 55)
    print(f"Total PDFs processed : {summary['total_pdfs']}")
    print(f"Total questions seen : {summary['total_questions']}")
    print(f"Total inserted       : {summary['total_inserted']}")
    print(f"Skipped (dupes)      : {summary['total_skipped_dupes']}")


def main() -> None:
    """CLI entrypoint for pipeline orchestration."""
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    summary = run_pipeline(
        manifest_path=args.manifest,
        db_path=args.db,
        dry_run=bool(args.dry_run),
        year=args.year,
        workers=max(1, int(args.workers)),
    )
    _print_summary(summary)


if __name__ == "__main__":
    main()
