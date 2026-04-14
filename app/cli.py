from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any

from groq import Groq
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from tabulate import tabulate

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from features.analytics import get_session_summary, log_attempt
from features.answer_key import generate_ai_explanation, get_answer
from features.flashcards import export_to_anki_txt, export_to_csv, generate_flashcard_deck
from features.mock_paper import export_paper_to_json, export_paper_to_txt, generate_mock_paper
from features.topic_bank import get_topic_stats, get_topic_tree
from pipeline.ingestor import init_db
from pipeline.run_pipeline import run_pipeline

console = Console()


def _resolve_db_path(arg_db: str | None) -> str:
    load_dotenv()
    env_db = os.environ.get("DB_PATH", "").strip()
    return arg_db or env_db or "data/db/questions.db"


def _build_client() -> Groq:
    load_dotenv()
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is required for this command")
    return Groq(api_key=api_key)


def _cmd_pipeline_run(args: argparse.Namespace) -> None:
    manifest_path = Path(args.manifest)
    total = 0
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            total = len(payload)

    progress_total = max(1, total)
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn()) as progress:
        task = progress.add_task("Running pipeline", total=progress_total)

        def _progress_hook(done: int, expected: int) -> None:
            progress.update(task, completed=min(done, max(1, expected)))

        summary = run_pipeline(
            manifest_path=manifest_path,
            db_path=Path(args.db),
            dry_run=bool(args.dry_run),
            year=args.year,
            workers=args.workers,
            progress_hook=_progress_hook,
        )

    table = [[key, value] for key, value in summary.items()]
    console.print(tabulate(table, headers=["Metric", "Value"], tablefmt="github"))


def _cmd_flashcards_generate(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    try:
        try:
            client = _build_client()
        except Exception as exc:
            client = None
            console.print(f"Groq client unavailable; using fallback answers. reason={exc}")
        cards = generate_flashcard_deck(
            filters={"topic": args.topic},
            conn=conn,
            client=client,
            limit=args.n,
        )
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        export_to_csv(cards, str(output_dir / "flashcards.csv"))
        export_to_anki_txt(cards, str(output_dir / "flashcards_anki.txt"))
        console.print(f"Generated {len(cards)} flashcards in {output_dir}")
    finally:
        conn.close()


def _cmd_paper_generate(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    try:
        config: dict[str, Any] = {
            "subject": args.subject,
            "topics": args.topics or [],
            "total_questions": args.n,
        }
        paper = generate_mock_paper(config=config, conn=conn)

        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        export_paper_to_txt(paper, str(out_dir / "mock_paper.txt"))
        export_paper_to_json(paper, str(out_dir / "mock_paper.json"))
        console.print(f"Generated paper {paper['paper_id']} with {len(paper['questions'])} questions")
    finally:
        conn.close()


def _cmd_topics_list(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    try:
        tree = get_topic_tree(conn)
        for subject, topics in tree.items():
            if args.subject and subject != args.subject:
                continue
            console.print(subject)
            for topic, subtopics in topics.items():
                console.print(f"  - {topic}")
                for subtopic, count in subtopics.items():
                    console.print(f"      - {subtopic}: {count}")
    finally:
        conn.close()


def _cmd_topics_stats(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    try:
        rows = get_topic_stats(conn=conn, subject=args.subject)
        console.print(tabulate(rows, headers="keys", tablefmt="github"))
    finally:
        conn.close()


def _cmd_answer_get(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    try:
        answer = get_answer(question_id=args.id, conn=conn)
        if answer is None:
            client = _build_client()
            answer = generate_ai_explanation(question_id=args.id, conn=conn, client=client)
        console.print(json.dumps(answer, indent=2, ensure_ascii=False))
    finally:
        conn.close()


def _cmd_analytics_summary(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    try:
        summary = get_session_summary(session_id=args.session, conn=conn)
        console.print(json.dumps(summary, indent=2, ensure_ascii=False))
    finally:
        conn.close()


def _cmd_attempt_log(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    try:
        value = str(args.correct).strip().lower() in {"true", "1", "yes", "y"}
        log_attempt(question_id=args.id, session_id=args.session, is_correct=value, conn=conn)
        console.print("Attempt logged")
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Question paper pipeline CLI")

    subparsers = parser.add_subparsers(dest="command", required=True)

    pipeline = subparsers.add_parser("pipeline")
    pipeline_sub = pipeline.add_subparsers(dest="pipeline_command", required=True)
    run_cmd = pipeline_sub.add_parser("run")
    run_cmd.add_argument("--manifest", default="data/manifest.json")
    run_cmd.add_argument("--db", default=None)
    run_cmd.add_argument("--dry-run", action="store_true")
    run_cmd.add_argument("--year", type=int, default=None)
    run_cmd.add_argument("--workers", type=int, default=5)
    run_cmd.set_defaults(handler=_cmd_pipeline_run)

    flash = subparsers.add_parser("flashcards")
    flash_sub = flash.add_subparsers(dest="flash_command", required=True)
    flash_gen = flash_sub.add_parser("generate")
    flash_gen.add_argument("--db", default=None)
    flash_gen.add_argument("--topic", required=True)
    flash_gen.add_argument("--n", type=int, default=20)
    flash_gen.add_argument("--output", default="exports/flashcards")
    flash_gen.set_defaults(handler=_cmd_flashcards_generate)

    paper = subparsers.add_parser("paper")
    paper_sub = paper.add_subparsers(dest="paper_command", required=True)
    paper_gen = paper_sub.add_parser("generate")
    paper_gen.add_argument("--db", default=None)
    paper_gen.add_argument("--subject", default=None)
    paper_gen.add_argument("--topics", nargs="*", default=[])
    paper_gen.add_argument("--n", type=int, default=30)
    paper_gen.add_argument("--output", default="exports/paper")
    paper_gen.set_defaults(handler=_cmd_paper_generate)

    topics = subparsers.add_parser("topics")
    topics_sub = topics.add_subparsers(dest="topics_command", required=True)
    topic_list = topics_sub.add_parser("list")
    topic_list.add_argument("--db", default=None)
    topic_list.add_argument("--subject", default=None)
    topic_list.set_defaults(handler=_cmd_topics_list)
    topic_stats = topics_sub.add_parser("stats")
    topic_stats.add_argument("--db", default=None)
    topic_stats.add_argument("--subject", default=None)
    topic_stats.set_defaults(handler=_cmd_topics_stats)

    answer = subparsers.add_parser("answer")
    answer_sub = answer.add_subparsers(dest="answer_command", required=True)
    answer_get = answer_sub.add_parser("get")
    answer_get.add_argument("--db", default=None)
    answer_get.add_argument("--id", type=int, required=True)
    answer_get.set_defaults(handler=_cmd_answer_get)

    analytics = subparsers.add_parser("analytics")
    analytics_sub = analytics.add_subparsers(dest="analytics_command", required=True)
    analytics_summary = analytics_sub.add_parser("summary")
    analytics_summary.add_argument("--db", default=None)
    analytics_summary.add_argument("--session", required=True)
    analytics_summary.set_defaults(handler=_cmd_analytics_summary)

    attempt = subparsers.add_parser("attempt")
    attempt_sub = attempt.add_subparsers(dest="attempt_command", required=True)
    attempt_log = attempt_sub.add_parser("log")
    attempt_log.add_argument("--db", default=None)
    attempt_log.add_argument("--id", type=int, required=True)
    attempt_log.add_argument("--session", required=True)
    attempt_log.add_argument("--correct", required=True)
    attempt_log.set_defaults(handler=_cmd_attempt_log)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if hasattr(args, "db"):
        args.db = _resolve_db_path(args.db)
    args.handler(args)


if __name__ == "__main__":
    main()
