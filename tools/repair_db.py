from __future__ import annotations

import argparse
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Iterable

JUNK_PATTERNS = [
    re.compile(r"(?i)^(solved\s+paper|aiims\s+explorer|neet\s+explorer)"),
    re.compile(r"(?i)time\s*:\s*\d"),
    re.compile(r"(?i)max\.?\s*marks"),
    re.compile(r"(?i)instructions\s+to\s+candidates"),
    re.compile(r"(?i)space\s+for\s+rough\s+work"),
    re.compile(r"(?i)page\s+\d+\s+of\s+\d+"),
]
MULTI_NUMBER_PATTERN = re.compile(r"(?<!\d)\d{1,2}\.")
TWO_COLUMN_LINE_PATTERN = re.compile(r"\d+\..*\s{3,}\d+\.")


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    """Create a SQLite connection with Row mapping enabled."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return a set of existing column names for a table."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row["name"]) for row in rows}


def add_missing_columns(conn: sqlite3.Connection, dry_run: bool) -> list[str]:
    """Add required nullable columns only when missing."""
    required = [
        ("has_figure", "INTEGER DEFAULT 0"),
        ("is_assertion_reason", "INTEGER DEFAULT 0"),
        ("question_number", "INTEGER"),
    ]

    present = existing_columns(conn, "questions")
    messages: list[str] = []

    for name, sql_type in required:
        if name in present:
            continue
        if dry_run:
            messages.append(f"  [schema] Would add column: {name}")
            continue
        conn.execute(f"ALTER TABLE questions ADD COLUMN {name} {sql_type}")
        messages.append(f"  [schema] Added column: {name}")

    if not messages:
        messages.append("  [schema] All required columns already exist")
    return messages


def ensure_indexes(conn: sqlite3.Connection, dry_run: bool) -> None:
    """Create query-performance indexes for common filter fields."""
    statements = [
        "CREATE INDEX IF NOT EXISTS idx_subject    ON questions(subject)",
        "CREATE INDEX IF NOT EXISTS idx_topic      ON questions(topic)",
        "CREATE INDEX IF NOT EXISTS idx_difficulty ON questions(difficulty)",
        "CREATE INDEX IF NOT EXISTS idx_year       ON questions(source_year)",
        "CREATE INDEX IF NOT EXISTS idx_type       ON questions(question_type)",
    ]
    if dry_run:
        return
    for statement in statements:
        conn.execute(statement)


def ensure_fts(conn: sqlite3.Connection, dry_run: bool) -> str:
    """Create FTS5 table and triggers if absent, then backfill existing rows."""
    found = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='questions_fts'"
    ).fetchone()
    if found:
        return "  [schema] FTS5 table already present"

    if dry_run:
        return "  [schema] Would create FTS5 table, triggers, and backfill existing rows"

    conn.executescript(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS questions_fts USING fts5(
            question_text, options, topic, subtopic,
            content='questions', content_rowid='id'
        );
        CREATE TRIGGER IF NOT EXISTS questions_ai AFTER INSERT ON questions BEGIN
            INSERT INTO questions_fts(rowid, question_text, options, topic, subtopic)
            VALUES (new.id, new.question_text, new.options, new.topic, new.subtopic);
        END;
        CREATE TRIGGER IF NOT EXISTS questions_ad AFTER DELETE ON questions BEGIN
            INSERT INTO questions_fts(questions_fts, rowid, question_text, options, topic, subtopic)
            VALUES ('delete', old.id, old.question_text, old.options, old.topic, old.subtopic);
        END;
        CREATE TRIGGER IF NOT EXISTS questions_au AFTER UPDATE ON questions BEGIN
            INSERT INTO questions_fts(questions_fts, rowid, question_text, options, topic, subtopic)
            VALUES ('delete', old.id, old.question_text, old.options, old.topic, old.subtopic);
            INSERT INTO questions_fts(rowid, question_text, options, topic, subtopic)
            VALUES (new.id, new.question_text, new.options, new.topic, new.subtopic);
        END;
        INSERT INTO questions_fts(rowid, question_text, options, topic, subtopic)
        SELECT id, question_text, options, topic, subtopic FROM questions;
        """
    )
    return "  [schema] Created FTS5 index and populated it"


def is_junk(text: str) -> bool:
    """Return True when text matches junk/header/fragment heuristics."""
    compact = (text or "").strip()
    if len(compact) < 15:
        return True

    first_line = compact.splitlines()[0] if compact else ""
    for pattern in JUNK_PATTERNS:
        if pattern.search(first_line) or pattern.search(compact):
            return True
    return False


def is_garbled(text: str) -> bool:
    """Return True for two-column merged OCR-like line artefacts."""
    compact = (text or "").strip()
    if not compact:
        return False

    if len(MULTI_NUMBER_PATTERN.findall(compact[:250])) >= 3:
        return True

    for line in compact.splitlines()[:6]:
        if TWO_COLUMN_LINE_PATTERN.search(line):
            return True

    return False


def classify_rows(rows: Iterable[sqlite3.Row]) -> tuple[list[int], list[int], Counter[int]]:
    """Classify row ids into junk and garbled buckets and summarize garbled by year."""
    junk_ids: list[int] = []
    garbled_ids: list[int] = []
    garbled_by_year: Counter[int] = Counter()

    for row in rows:
        row_id = int(row["id"])
        text = str(row["question_text"] or "")
        year = int(row["source_year"] or 0)

        if is_junk(text):
            junk_ids.append(row_id)
            continue

        if is_garbled(text):
            garbled_ids.append(row_id)
            garbled_by_year[year] += 1

    return junk_ids, garbled_ids, garbled_by_year


def delete_junk_rows(conn: sqlite3.Connection, junk_ids: list[int], dry_run: bool) -> int:
    """Delete junk row ids unless dry-run is enabled; return deleted count."""
    if dry_run or not junk_ids:
        return 0

    with conn:
        conn.executemany("DELETE FROM questions WHERE id = ?", [(row_id,) for row_id in junk_ids])
    return len(junk_ids)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the DB repair tool."""
    parser = argparse.ArgumentParser(description="Repair and classify rows in questions.db")
    parser.add_argument("--db", default="data/db/questions.db", help="Path to SQLite database")
    parser.add_argument("--dry-run", action="store_true", help="Report only; do not modify DB")
    return parser.parse_args()


def main() -> None:
    """Run schema repair, junk cleanup, and garbled detection report."""
    args = parse_args()
    db_path = Path(args.db)

    conn = connect_db(db_path)
    try:
        total_rows = int(conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0])
        print(f"Total rows in DB: {total_rows}\n")

        print("[1/4] Checking schema...")
        if args.dry_run:
            column_msgs = add_missing_columns(conn, dry_run=True)
            for msg in column_msgs:
                print(msg)
            print("  [schema] Would ensure secondary indexes")
            print(ensure_fts(conn, dry_run=True))
        else:
            with conn:
                column_msgs = add_missing_columns(conn, dry_run=False)
                for msg in column_msgs:
                    print(msg)
                ensure_indexes(conn, dry_run=False)
                print("  [schema] Ensured secondary indexes")
                print(ensure_fts(conn, dry_run=False))

        print("\n[2/4] Classifying rows...")
        rows = conn.execute("SELECT id, question_text, source_year FROM questions").fetchall()
        junk_ids, garbled_ids, garbled_by_year = classify_rows(rows)

        junk_count = len(junk_ids)
        garbled_count = len(garbled_ids)
        potentially_usable = max(0, total_rows - junk_count - garbled_count)

        print(f"  Junk rows (headers, covers, fragments): {junk_count}")
        print(f"  Two-column garbled rows:                {garbled_count}")
        print(f"  Potentially usable rows:                {potentially_usable}")

        print(f"\n[3/4] Deleting {junk_count} junk rows...")
        deleted = delete_junk_rows(conn, junk_ids, dry_run=bool(args.dry_run))
        if args.dry_run:
            print(f"  Dry-run only. Would delete {junk_count} junk rows.")
        else:
            print(f"  Deleted {deleted} junk rows.")

        print("\n[4/4] Garbled rows by year (need re-extraction):")
        if garbled_by_year:
            for year in sorted(garbled_by_year):
                year_label = str(year) if year else "unknown"
                print(f"  {year_label}: {garbled_by_year[year]} garbled rows")
        else:
            print("  None")

        rows_after = total_rows - junk_count if args.dry_run else int(conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0])

        print("\n── SUMMARY ─────────────────────────────────────────")
        print(f"  Rows before:        {total_rows}")
        junk_removed_label = f"{junk_count} (planned)" if args.dry_run else str(junk_count)
        print(f"  Junk removed:       {junk_removed_label}")
        print(f"  Garbled (kept):     {garbled_count}")
        print(f"  Clean & taggable:   {max(0, rows_after - garbled_count)}")
        suffix = " (projected)" if args.dry_run else ""
        print(f"  Rows after repair{suffix}:  {rows_after}")

        print("\n  NEXT STEP: Run tools/retag.py to tag the clean rows with Groq.")
        print("  NOTE: Garbled rows from scanned 2-column PDFs need proper OCR for accurate extraction.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
