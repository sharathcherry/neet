from __future__ import annotations

import pytest

from pipeline.extractor import PAGE_BREAK_SENTINEL
from pipeline.splitter import split_questions


def test_splitter_detects_mcq() -> None:
    text = """
    Q.1 What is 2+2?
    A. 3
    B. 4
    C. 5
    D. 6

    Q.2 What is 3+3?
    A. 5
    B. 6
    C. 7
    D. 8
    """
    rows = split_questions(text, {"source_year": 2020, "source_pdf": "x.pdf"})
    assert len(rows) >= 2
    assert rows[0]["question_type"] == "mcq"
    assert len(rows[0]["options"]) == 4


def test_splitter_detects_short_answer() -> None:
    text = """
    Question 1 Define osmosis?

    Question 2 Explain photosynthesis process.
    """
    rows = split_questions(text, {"source_year": 2021, "source_pdf": "x.pdf"})
    assert rows[0]["question_type"] == "short_answer"


def test_splitter_empty_input_raises() -> None:
    with pytest.raises(ValueError):
        split_questions("", {"source_year": 2022, "source_pdf": "x.pdf"})


def test_splitter_handles_malformed_numbers() -> None:
    text = """
    1) State Ohm's law.
    3) List two greenhouse gases.
    """
    rows = split_questions(text, {"source_year": 2019, "source_pdf": "x.pdf"})
    assert len(rows) == 2
    assert rows[0]["question_number"] == 1
    assert rows[1]["question_number"] == 3


def test_splitter_multi_page_page_hint() -> None:
    text = (
        "Q.1 First page question?\nA. 1\nB. 2\nC. 3\nD. 4"
        + PAGE_BREAK_SENTINEL
        + "Q.2 Second page question?\nA. a\nB. b\nC. c\nD. d"
    )
    rows = split_questions(text, {"source_year": 2018, "source_pdf": "x.pdf"})
    assert rows[0]["page_hint"] == 1
    assert rows[1]["page_hint"] == 2
