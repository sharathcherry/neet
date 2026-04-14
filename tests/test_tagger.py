from __future__ import annotations

import pytest

from pipeline.tagger import _sanitize_tags


def test_sanitize_tags_downgrades_invalid_values() -> None:
    payload = {
        "topic": "Cell Cycle",
        "subtopic": "Mitosis",
        "difficulty": "intermediate",
        "bloom_level": "synthesize",
        "subject": "biology",
        "tag_confidence": 1.7,
    }

    result = _sanitize_tags(payload)
    assert result["difficulty"] == "unknown"
    assert result["bloom_level"] == "unknown"
    assert result["subject"] == "Unknown"
    assert result["tag_confidence"] == 1.0


def test_sanitize_tags_accepts_valid_payload() -> None:
    payload = {
        "topic": "Cell Cycle",
        "subtopic": "Mitosis",
        "difficulty": "medium",
        "bloom_level": "apply",
        "subject": "Botany",
        "tag_confidence": 0.73,
    }

    result = _sanitize_tags(payload)
    assert result["topic"] == "Cell Cycle"
    assert result["subtopic"] == "Mitosis"
    assert result["difficulty"] == "medium"
    assert result["bloom_level"] == "apply"
    assert result["subject"] == "Botany"
    assert result["tag_confidence"] == 0.73


def test_sanitize_tags_requires_all_keys() -> None:
    with pytest.raises(ValueError):
        _sanitize_tags({"topic": "Cell Cycle"})
