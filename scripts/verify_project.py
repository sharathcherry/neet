from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from verification import run_project_verification


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run project-wide integrity verification checks.")
    parser.add_argument("--project-root", type=Path, default=ROOT)
    parser.add_argument("--deep-pdf-scan", action="store_true", help="Validate PDF signatures for all scraped PDFs.")
    parser.add_argument(
        "--pdf-sample-limit",
        type=int,
        default=30,
        help="When deep scan is off, number of PDFs to sample for signature checks.",
    )
    parser.add_argument(
        "--verify-remote-sources",
        action="store_true",
        help="Fetch source URLs and verify answer-key/solution metadata from page title/headings.",
    )
    parser.add_argument(
        "--remote-sample-limit",
        type=int,
        default=0,
        help="Limit number of remote source URLs to verify (0 means all).",
    )
    parser.add_argument(
        "--remote-timeout-seconds",
        type=int,
        default=20,
        help="Timeout for remote source metadata requests.",
    )
    parser.add_argument("--output-json", type=Path, default=None, help="Optional path to write full JSON report.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = run_project_verification(
        project_root=args.project_root,
        deep_pdf_scan=bool(args.deep_pdf_scan),
        pdf_sample_limit=max(1, int(args.pdf_sample_limit)),
        verify_remote_sources=bool(args.verify_remote_sources),
        remote_sample_limit=max(0, int(args.remote_sample_limit)),
        remote_timeout_seconds=max(5, int(args.remote_timeout_seconds)),
    )

    print("[VERIFICATION SUMMARY]")
    print(f"status: {report['status']}")
    print(f"passed: {report['passed']}")
    print(f"warnings: {report['warnings']}")
    print(f"failed: {report['failed']}")

    print("\n[CHECKS]")
    for check in report.get("checks", []):
        print(f"[{str(check.get('status', '')).upper()}] {check.get('name', 'unknown')}: {check.get('message', '')}")

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nFull report written to: {args.output_json}")

    # Non-zero exit only when there are hard failures.
    if int(report.get("failed", 0)) > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
