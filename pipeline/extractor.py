from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Any

import pdfplumber

PAGE_BREAK_SENTINEL = "\n<<<PAGE_BREAK>>>\n"


def _clean_page_text(text: str) -> str:
    """Normalize noisy whitespace while preserving line breaks between blocks."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_text(pdf_path: str) -> dict[str, Any]:
    """Extract text from a PDF and return raw text with page metadata.

    Args:
        pdf_path: Absolute or relative path to a PDF file.

    Returns:
        A dictionary containing raw_text, page_count, and source_path.
        On failure, includes error and an empty raw_text.
    """
    source_path = str(pdf_path)
    path = Path(pdf_path)

    try:
        page_texts: list[str] = []
        with pdfplumber.open(path) as pdf:
            page_count = len(pdf.pages)
            for page_number, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text(layout=True) or ""
                page_text = _clean_page_text(page_text)
                if not page_text:
                    logging.warning("No text extracted from page %s of %s", page_number, source_path)
                    continue
                page_texts.append(page_text)

        return {
            "raw_text": PAGE_BREAK_SENTINEL.join(page_texts),
            "page_count": page_count,
            "source_path": source_path,
        }
    except Exception as exc:
        return {
            "error": str(exc),
            "raw_text": "",
            "source_path": source_path,
            "page_count": 0,
        }


def _build_cli_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for extractor script execution."""
    parser = argparse.ArgumentParser(description="Extract text from a question paper PDF.")
    parser.add_argument("pdf_path", help="Path to the PDF file")
    return parser


def main() -> None:
    """Run extraction from CLI and print a short preview."""
    parser = _build_cli_parser()
    args = parser.parse_args()

    result = extract_text(args.pdf_path)
    if result.get("error"):
        print(f"ERROR: {result['error']}")
        return

    print(f"Pages: {result.get('page_count', 0)}")
    print(result.get("raw_text", "")[:500])


if __name__ == "__main__":
    main()
