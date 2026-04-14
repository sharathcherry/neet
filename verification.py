from __future__ import annotations

import csv
import importlib.util
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASS = "pass"
WARN = "warn"
FAIL = "fail"


def _make_check(name: str, status: str, message: str, metrics: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "message": message,
        "metrics": metrics or {},
    }


def _status_rank(status: str) -> int:
    if status == FAIL:
        return 2
    if status == WARN:
        return 1
    return 0


def _max_status(a: str, b: str) -> str:
    return a if _status_rank(a) >= _status_rank(b) else b


def _load_json(path: Path) -> tuple[Any | None, str | None]:
    if not path.exists():
        return None, f"missing file: {path}"

    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except Exception as exc:
        return None, f"invalid json: {exc}"


def _parse_requirements(requirements_path: Path) -> list[str]:
    if not requirements_path.exists():
        return []

    package_names: list[str] = []
    for raw_line in requirements_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        package = re.split(r"[<>=!~]", line, maxsplit=1)[0].strip()
        if package:
            package_names.append(package)

    return package_names


def _verify_python_files(project_root: Path) -> dict[str, Any]:
    targets = [
        project_root / "app.py",
        project_root / "study_utils.py",
        project_root / "verification.py",
        project_root / "scripts" / "scrape_neet_papers.py",
        project_root / "scripts" / "verify_project.py",
    ]

    checked = 0
    failures: list[str] = []

    for path in targets:
        if not path.exists():
            continue
        checked += 1
        try:
            source = path.read_text(encoding="utf-8")
            compile(source, str(path), "exec")
        except Exception as exc:
            failures.append(f"{path}: {exc}")

    if failures:
        return _make_check(
            name="Python Syntax",
            status=FAIL,
            message="Syntax validation failed for one or more Python files.",
            metrics={"checked_files": checked, "failures": failures[:10], "failure_count": len(failures)},
        )

    return _make_check(
        name="Python Syntax",
        status=PASS,
        message="Core Python files compile successfully.",
        metrics={"checked_files": checked},
    )


def _verify_dependencies(project_root: Path) -> dict[str, Any]:
    requirements_path = project_root / "requirements.txt"
    packages = _parse_requirements(requirements_path)

    # Package name -> import name mapping for common mismatches.
    import_map = {
        "beautifulsoup4": "bs4",
    }

    checked = 0
    missing: list[str] = []

    for package in packages:
        module_name = import_map.get(package.lower(), package)
        checked += 1
        if importlib.util.find_spec(module_name) is None:
            missing.append(package)

    if missing:
        return _make_check(
            name="Dependencies",
            status=FAIL,
            message="Some required packages are not importable in the current environment.",
            metrics={"requirements_file": str(requirements_path), "checked": checked, "missing": missing},
        )

    if not packages:
        return _make_check(
            name="Dependencies",
            status=WARN,
            message="requirements.txt is missing or empty.",
            metrics={"requirements_file": str(requirements_path)},
        )

    return _make_check(
        name="Dependencies",
        status=PASS,
        message="All packages declared in requirements.txt are importable.",
        metrics={"checked": checked},
    )


def _verify_questions(project_root: Path) -> tuple[dict[str, Any], set[str]]:
    questions_path = project_root / "data" / "questions.json"
    payload, error = _load_json(questions_path)

    if error:
        return (
            _make_check(
                name="Question Bank",
                status=FAIL,
                message="questions.json is missing or invalid.",
                metrics={"path": str(questions_path), "error": error},
            ),
            set(),
        )

    if not isinstance(payload, list):
        return (
            _make_check(
                name="Question Bank",
                status=FAIL,
                message="questions.json must contain a list of question objects.",
                metrics={"path": str(questions_path), "type": type(payload).__name__},
            ),
            set(),
        )

    required_fields = {"id", "exam_id", "subject", "text", "options"}
    duplicate_ids = 0
    malformed_rows = 0
    bad_options = 0
    usable_count = 0

    seen_ids: set[str] = set()
    valid_ids: set[str] = set()

    for row in payload:
        if not isinstance(row, dict):
            malformed_rows += 1
            continue

        missing_fields = [field for field in required_fields if field not in row]
        if missing_fields:
            malformed_rows += 1

        qid = str(row.get("id", "")).strip()
        if not qid:
            malformed_rows += 1
        elif qid in seen_ids:
            duplicate_ids += 1
        else:
            seen_ids.add(qid)
            valid_ids.add(qid)

        options = row.get("options")
        if not isinstance(options, list) or len(options) < 4:
            bad_options += 1

        if row.get("is_usable", True):
            usable_count += 1

    status = PASS
    message = "Question bank structure looks valid."

    if malformed_rows > 0 or duplicate_ids > 0:
        status = FAIL
        message = "Question bank has malformed rows or duplicate IDs."
    elif bad_options > 0:
        status = WARN
        message = "Some questions have incomplete options arrays."

    return (
        _make_check(
            name="Question Bank",
            status=status,
            message=message,
            metrics={
                "path": str(questions_path),
                "question_count": len(payload),
                "usable_questions": usable_count,
                "malformed_rows": malformed_rows,
                "duplicate_ids": duplicate_ids,
                "rows_with_incomplete_options": bad_options,
            },
        ),
        valid_ids,
    )


def _verify_state(project_root: Path, valid_question_ids: set[str]) -> dict[str, Any]:
    state_path = project_root / "data" / "study_state.json"
    payload, error = _load_json(state_path)

    if error:
        return _make_check(
            name="Study State",
            status=FAIL,
            message="study_state.json is missing or invalid.",
            metrics={"path": str(state_path), "error": error},
        )

    if not isinstance(payload, dict):
        return _make_check(
            name="Study State",
            status=FAIL,
            message="study_state.json must contain a JSON object.",
            metrics={"path": str(state_path), "type": type(payload).__name__},
        )

    users = payload.get("users", {})
    answer_key = payload.get("answer_key", {})

    if not isinstance(users, dict) or not isinstance(answer_key, dict):
        return _make_check(
            name="Study State",
            status=FAIL,
            message="State object must contain dict fields: users and answer_key.",
            metrics={
                "users_type": type(users).__name__,
                "answer_key_type": type(answer_key).__name__,
            },
        )

    required_user_lists = [
        "attempts",
        "exam_history",
        "daily_quiz_history",
        "flashcards",
        "revision_plan",
        "omr_history",
        "pyq_history",
    ]

    user_shape_errors = 0
    for user_name, user_obj in users.items():
        if not isinstance(user_obj, dict):
            user_shape_errors += 1
            continue
        for key in required_user_lists:
            if key not in user_obj or not isinstance(user_obj.get(key), list):
                user_shape_errors += 1

    unknown_answer_keys = 0
    if valid_question_ids:
        unknown_answer_keys = sum(1 for qid in answer_key.keys() if qid not in valid_question_ids)

    status = PASS
    message = "State file structure is valid."

    if user_shape_errors > 0:
        status = FAIL
        message = "One or more user records are malformed."
    elif unknown_answer_keys > 0:
        status = WARN
        message = "Answer key contains question IDs not present in questions.json."

    return _make_check(
        name="Study State",
        status=status,
        message=message,
        metrics={
            "path": str(state_path),
            "users": len(users),
            "answer_key_entries": len(answer_key),
            "unknown_answer_key_entries": unknown_answer_keys,
            "user_shape_errors": user_shape_errors,
        },
    )


def _is_pdf_signature(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            return handle.read(4) == b"%PDF"
    except Exception:
        return False


def _verify_scrape_artifacts(project_root: Path, deep_pdf_scan: bool = False, pdf_sample_limit: int = 30) -> dict[str, Any]:
    scrape_dir = project_root / "data" / "neet_papers"
    manifest_path = scrape_dir / "manifest.csv"
    summary_path = scrape_dir / "summary.json"
    papers_dir = scrape_dir / "papers"

    if not scrape_dir.exists():
        return _make_check(
            name="Scraped Papers",
            status=WARN,
            message="Scraped paper directory not found; skipping scrape integrity checks.",
            metrics={"path": str(scrape_dir)},
        )

    if not manifest_path.exists():
        return _make_check(
            name="Scraped Papers",
            status=FAIL,
            message="manifest.csv is missing from scraped paper artifacts.",
            metrics={"manifest_path": str(manifest_path)},
        )

    rows: list[dict[str, str]] = []
    try:
        with manifest_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except Exception as exc:
        return _make_check(
            name="Scraped Papers",
            status=FAIL,
            message="manifest.csv cannot be parsed.",
            metrics={"manifest_path": str(manifest_path), "error": str(exc)},
        )

    summary_payload, summary_error = _load_json(summary_path)

    status_counts: dict[str, int] = {}
    successful_rows = 0
    missing_files = 0

    for row in rows:
        row_status = str(row.get("status", "")).strip().lower() or "unknown"
        status_counts[row_status] = status_counts.get(row_status, 0) + 1

        if row_status in {"ok", "exists"}:
            successful_rows += 1
            file_path_raw = str(row.get("file_path", "")).strip()
            file_path = Path(file_path_raw) if file_path_raw else None
            if not file_path or not file_path.exists():
                missing_files += 1

    disk_files = sorted([path for path in papers_dir.glob("*.pdf")]) if papers_dir.exists() else []

    files_to_scan = disk_files if deep_pdf_scan else disk_files[: max(1, int(pdf_sample_limit))]
    bad_pdf_signatures = 0
    for pdf_path in files_to_scan:
        if not _is_pdf_signature(pdf_path):
            bad_pdf_signatures += 1

    overall_status = PASS
    message = "Scraped paper artifacts look consistent."

    if missing_files > 0 or bad_pdf_signatures > 0:
        overall_status = FAIL
        message = "Some scraped PDFs are missing or malformed."

    summary_mismatch = False
    if summary_error:
        overall_status = _max_status(overall_status, WARN)
        message = "Summary file missing/invalid; manifest checks completed."
    elif isinstance(summary_payload, dict):
        reported_downloaded = int(summary_payload.get("downloaded_files", -1))
        if reported_downloaded != successful_rows:
            summary_mismatch = True
            overall_status = _max_status(overall_status, WARN)

    return _make_check(
        name="Scraped Papers",
        status=overall_status,
        message=message,
        metrics={
            "manifest_path": str(manifest_path),
            "summary_path": str(summary_path),
            "papers_dir": str(papers_dir),
            "manifest_rows": len(rows),
            "successful_rows": successful_rows,
            "files_on_disk": len(disk_files),
            "missing_success_files": missing_files,
            "scanned_pdf_headers": len(files_to_scan),
            "bad_pdf_signatures": bad_pdf_signatures,
            "status_counts": status_counts,
            "summary_mismatch": summary_mismatch,
            "summary_error": summary_error,
            "deep_pdf_scan": bool(deep_pdf_scan),
        },
    )


def _verify_remote_source_metadata(
    project_root: Path,
    enabled: bool,
    remote_sample_limit: int,
    remote_timeout_seconds: int,
) -> dict[str, Any]:
    if not enabled:
        return _make_check(
            name="Remote Source Metadata",
            status=WARN,
            message="Remote source verification disabled.",
            metrics={"enabled": False},
        )

    manifest_path = project_root / "data" / "neet_papers" / "manifest.csv"
    if not manifest_path.exists():
        return _make_check(
            name="Remote Source Metadata",
            status=FAIL,
            message="manifest.csv is required for remote source verification.",
            metrics={"manifest_path": str(manifest_path)},
        )

    try:
        import requests
        from bs4 import BeautifulSoup
    except Exception as exc:
        return _make_check(
            name="Remote Source Metadata",
            status=FAIL,
            message="requests and beautifulsoup4 are required for remote verification.",
            metrics={"error": str(exc)},
        )

    rows: list[dict[str, str]] = []
    with manifest_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    source_urls: list[str] = []
    for row in rows:
        source_url = str(row.get("source_url", "")).strip()
        if source_url:
            source_urls.append(source_url)

    if remote_sample_limit > 0:
        source_urls = source_urls[:remote_sample_limit]

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    pattern = re.compile(r"answer\s*key|solutions?|with\s*answer", re.IGNORECASE)

    checked = 0
    matched = 0
    failures = 0

    for url in source_urls:
        checked += 1
        try:
            response = session.get(url, timeout=remote_timeout_seconds)
            if response.status_code != 200:
                failures += 1
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            title_text = soup.title.text.strip() if soup.title else ""
            heading_text = " ".join(node.get_text(" ", strip=True) for node in soup.select("h1, h2")[:2])
            if pattern.search(f"{title_text} {heading_text}"):
                matched += 1
        except Exception:
            failures += 1

    status = PASS
    message = "Remote source metadata indicates answer-key/solution pages."

    if checked == 0:
        status = WARN
        message = "No source URLs found to verify remotely."
    elif matched < checked:
        status = WARN
        message = "Some source pages do not clearly indicate answer-key metadata."

    return _make_check(
        name="Remote Source Metadata",
        status=status,
        message=message,
        metrics={
            "enabled": True,
            "checked": checked,
            "matched": matched,
            "failures": failures,
            "coverage_pct": round((matched / checked) * 100, 2) if checked else 0.0,
            "remote_timeout_seconds": remote_timeout_seconds,
        },
    )


def run_project_verification(
    project_root: str | Path = ".",
    deep_pdf_scan: bool = False,
    pdf_sample_limit: int = 30,
    verify_remote_sources: bool = False,
    remote_sample_limit: int = 0,
    remote_timeout_seconds: int = 20,
) -> dict[str, Any]:
    root = Path(project_root).resolve()

    checks: list[dict[str, Any]] = []
    checks.append(_verify_python_files(root))
    checks.append(_verify_dependencies(root))

    questions_check, valid_question_ids = _verify_questions(root)
    checks.append(questions_check)

    checks.append(_verify_state(root, valid_question_ids=valid_question_ids))
    checks.append(_verify_scrape_artifacts(root, deep_pdf_scan=deep_pdf_scan, pdf_sample_limit=pdf_sample_limit))
    checks.append(
        _verify_remote_source_metadata(
            root,
            enabled=verify_remote_sources,
            remote_sample_limit=max(0, int(remote_sample_limit)),
            remote_timeout_seconds=max(5, int(remote_timeout_seconds)),
        )
    )

    passed = sum(1 for check in checks if check["status"] == PASS)
    warnings = sum(1 for check in checks if check["status"] == WARN)
    failed = sum(1 for check in checks if check["status"] == FAIL)

    overall_status = PASS
    if failed:
        overall_status = FAIL
    elif warnings:
        overall_status = WARN

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project_root": str(root),
        "status": overall_status,
        "passed": passed,
        "warnings": warnings,
        "failed": failed,
        "checks": checks,
        "config": {
            "deep_pdf_scan": bool(deep_pdf_scan),
            "pdf_sample_limit": max(1, int(pdf_sample_limit)),
            "verify_remote_sources": bool(verify_remote_sources),
            "remote_sample_limit": max(0, int(remote_sample_limit)),
            "remote_timeout_seconds": max(5, int(remote_timeout_seconds)),
        },
    }
