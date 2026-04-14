from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    """Create a SQLite connection with row mapping enabled."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the tag-audit report."""
    parser = argparse.ArgumentParser(description="Audit tag quality and coverage in questions.db")
    parser.add_argument("--db", default="data/db/questions.db", help="Path to SQLite database")
    return parser.parse_args()


def bar(count: int, max_count: int, max_width: int = 30) -> str:
    """Build a proportional unicode bar for confidence distribution rows."""
    if max_count <= 0 or count <= 0:
        return ""
    width = int(round((count / max_count) * max_width))
    width = max(1, min(max_width, width))
    return "█" * width


def print_report(conn: sqlite3.Connection) -> None:
    """Print the full SQL-driven audit report to stdout."""
    totals = conn.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN tag_confidence > 0.0 THEN 1 ELSE 0 END) AS tagged_rows,
            SUM(CASE WHEN tag_confidence <= 0.0 OR tag_confidence IS NULL THEN 1 ELSE 0 END) AS untagged_rows
        FROM questions
        """
    ).fetchone()

    by_subject = conn.execute(
        """
        SELECT COALESCE(subject, 'unknown') AS subject, COUNT(*) AS row_count
        FROM questions
        GROUP BY COALESCE(subject, 'unknown')
        ORDER BY row_count DESC
        """
    ).fetchall()

    by_difficulty = conn.execute(
        """
        SELECT COALESCE(difficulty, 'unknown') AS difficulty, COUNT(*) AS row_count
        FROM questions
        GROUP BY COALESCE(difficulty, 'unknown')
        ORDER BY row_count DESC
        """
    ).fetchall()

    by_year = conn.execute(
        """
        SELECT COALESCE(source_year, 0) AS source_year, COUNT(*) AS row_count
        FROM questions
        GROUP BY COALESCE(source_year, 0)
        ORDER BY source_year ASC
        """
    ).fetchall()

    top_topics = conn.execute(
        """
        SELECT topic, COUNT(*) AS row_count
        FROM questions
        WHERE LOWER(COALESCE(topic, 'unknown')) != 'unknown'
        GROUP BY topic
        ORDER BY row_count DESC
        LIMIT 15
        """
    ).fetchall()

    confidence_rows = conn.execute(
        """
        SELECT
            CASE
                WHEN tag_confidence >= 0.9 THEN '0.90–1.00 (high)'
                WHEN tag_confidence >= 0.7 THEN '0.70–0.89 (good)'
                WHEN tag_confidence >= 0.5 THEN '0.50–0.69 (ok)'
                WHEN tag_confidence >  0.0 THEN '0.01–0.49 (low)'
                ELSE                           '0.00 (untagged)'
            END AS confidence_band,
            COUNT(*) AS row_count
        FROM questions
        GROUP BY confidence_band
        ORDER BY CASE confidence_band
            WHEN '0.90–1.00 (high)' THEN 1
            WHEN '0.70–0.89 (good)' THEN 2
            WHEN '0.50–0.69 (ok)' THEN 3
            WHEN '0.01–0.49 (low)' THEN 4
            ELSE 5
        END
        """
    ).fetchall()

    total_rows = int(totals["total_rows"] or 0)
    tagged_rows = int(totals["tagged_rows"] or 0)
    untagged_rows = int(totals["untagged_rows"] or 0)
    tagged_pct = round((tagged_rows / total_rows) * 100) if total_rows else 0

    print("""──────────────────────────────────────────────────
  NEET Question Bank — Tag Audit Report
──────────────────────────────────────────────────""")
    print(f"  Total questions:   {total_rows:,}")
    print(f"  Tagged (conf>0):   {tagged_rows:,}  ({tagged_pct}%)")
    print(f"  Untagged:          {untagged_rows:,}\n")

    print("  By Subject:")
    for row in by_subject:
        print(f"    {str(row['subject']):<20} {int(row['row_count']):>6,}")

    print("\n  By Difficulty:")
    for row in by_difficulty:
        print(f"    {str(row['difficulty']):<15} {int(row['row_count']):>6,}")

    print("\n  By Year:")
    for row in by_year:
        year = int(row["source_year"] or 0)
        year_label = str(year) if year else "unknown"
        print(f"    {year_label}:  {int(row['row_count']):,}")

    print("\n  Top 15 Topics:")
    if top_topics:
        for row in top_topics:
            print(f"    {str(row['topic']):<40} {int(row['row_count']):>6,}")
    else:
        print("    No non-unknown topics available.")

    print("\n  Confidence Distribution:")
    max_band_count = max((int(row["row_count"]) for row in confidence_rows), default=0)
    for row in confidence_rows:
        label = str(row["confidence_band"])
        count = int(row["row_count"])
        print(f"    {label:<24} {count:>6,}  {bar(count, max_band_count)}")


def main() -> None:
    """CLI entrypoint for DB tag quality audit."""
    args = parse_args()
    conn = connect_db(args.db)
    try:
        print_report(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
