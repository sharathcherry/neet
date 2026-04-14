from __future__ import annotations

from pathlib import Path

from reportlab.pdfgen import canvas

from pipeline.extractor import extract_text


def _make_fixture_pdf(path: Path) -> None:
    pdf = canvas.Canvas(str(path))
    pdf.drawString(72, 760, "Q.1 What is velocity?")
    pdf.drawString(72, 740, "A. speed with direction")
    pdf.showPage()
    pdf.drawString(72, 760, "Question 2 Explain inertia.")
    pdf.save()


def test_extract_text_success(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.pdf"
    _make_fixture_pdf(fixture)

    result = extract_text(str(fixture))
    assert result.get("error") is None
    assert result["page_count"] == 2
    assert "velocity" in result["raw_text"]


def test_extract_text_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.pdf"
    result = extract_text(str(missing))
    assert result["raw_text"] == ""
    assert "error" in result
