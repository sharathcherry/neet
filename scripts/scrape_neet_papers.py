from __future__ import annotations

import argparse
import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

DOMAIN_PRIORITY = {
    "neet.nta.nic.in": 100,
    "nta.ac.in": 100,
    "ntaonline.in": 95,
    "ncert.nic.in": 80,
    "archive.org": 75,
    "resonance.ac.in": 70,
    "byjus.com": 55,
    "vedantu.com": 55,
    "career360.com": 50,
    "embibe.com": 45,
    "selfstudys.com": 35,
}

SELFSTUDYS_BASE_URL = "https://www.selfstudys.com/books/neet-previous-year-paper"
SELFSTUDYS_NEET_SITEMAP = "https://www.selfstudys.com/sitemaps/neet.xml"


@dataclass
class CandidateLink:
    year: int
    query: str
    source_url: str
    pdf_url: str
    score: int
    domain: str


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    return session


def domain_score(url: str) -> int:
    domain = urlparse(url).netloc.lower()
    base = DOMAIN_PRIORITY.get(domain, 20)
    if domain.startswith("www."):
        base = max(base, DOMAIN_PRIORITY.get(domain[4:], base))
    return base


def normalize_result_url(url: str) -> str:
    parsed = urlparse(url)
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        query = parse_qs(parsed.query)
        uddg_values = query.get("uddg")
        if uddg_values:
            return unquote(uddg_values[0])
    return url


def html_search_duckduckgo(session: requests.Session, query: str, limit: int = 20) -> list[str]:
    search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    response = session.get(search_url, timeout=25)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    results: list[str] = []

    for anchor in soup.select("a.result__a"):
        href = anchor.get("href", "").strip()
        if not href:
            continue
        normalized = normalize_result_url(href)
        if normalized.startswith("http"):
            results.append(normalized)
        if len(results) >= limit:
            break

    return results


def looks_like_pdf_url(url: str) -> bool:
    cleaned = url.lower().split("?")[0]
    return cleaned.endswith(".pdf")


def extract_pdf_links_from_page(session: requests.Session, page_url: str) -> list[str]:
    try:
        response = session.get(page_url, timeout=25)
    except requests.RequestException:
        return []

    if response.status_code != 200:
        return []

    content_type = response.headers.get("Content-Type", "").lower()
    if "application/pdf" in content_type:
        return [page_url]

    soup = BeautifulSoup(response.text, "html.parser")
    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if not href:
            continue
        absolute = urljoin(page_url, href)
        if looks_like_pdf_url(absolute):
            links.append(absolute)

    return links


def build_queries_for_year(year: int) -> list[str]:
    queries = [
        f"NEET question paper {year} pdf",
        f"NEET UG question paper {year} pdf with answer key",
    ]
    if year <= 2012:
        queries.extend(
            [
                f"AIPMT question paper {year} pdf",
                f"AIPMT medical entrance question paper {year} pdf",
            ]
        )
    return queries


def score_candidate(pdf_url: str, query: str, year: int) -> int:
    score = domain_score(pdf_url)
    lower_url = pdf_url.lower()
    lower_query = query.lower()

    if looks_like_pdf_url(pdf_url):
        score += 20
    if str(year) in lower_url:
        score += 15
    if "question" in lower_url:
        score += 10
    if "answer" in lower_url or "key" in lower_url:
        score += 2
    if "official" in lower_query or "nta" in lower_query:
        score += 5

    return score


def discover_candidates_for_year(
    session: requests.Session,
    year: int,
    search_limit: int,
    page_pdf_probe_limit: int,
) -> list[CandidateLink]:
    candidates: list[CandidateLink] = []
    seen: set[str] = set()

    for query in build_queries_for_year(year):
        try:
            result_links = html_search_duckduckgo(session, query, limit=search_limit)
        except requests.RequestException:
            continue

        for idx, result_url in enumerate(result_links):
            discovered_pdfs: list[str] = []
            if looks_like_pdf_url(result_url):
                discovered_pdfs = [result_url]
            elif idx < page_pdf_probe_limit:
                discovered_pdfs = extract_pdf_links_from_page(session, result_url)

            for pdf_url in discovered_pdfs:
                clean_url = pdf_url.strip()
                if not clean_url or clean_url in seen:
                    continue
                seen.add(clean_url)

                score = score_candidate(clean_url, query, year)
                domain = urlparse(clean_url).netloc.lower()
                candidates.append(
                    CandidateLink(
                        year=year,
                        query=query,
                        source_url=result_url,
                        pdf_url=clean_url,
                        score=score,
                        domain=domain,
                    )
                )

        time.sleep(0.4)

    candidates.sort(key=lambda item: item.score, reverse=True)
    return candidates


def discover_selfstudys_year_pages(
    session: requests.Session,
    start_year: int,
    end_year: int,
) -> dict[int, list[str]]:
    try:
        response = session.get(SELFSTUDYS_NEET_SITEMAP, timeout=40)
        response.raise_for_status()
    except requests.RequestException:
        return {}

    text = response.text
    raw_links = [
        match.group(1).strip()
        for match in re.finditer(r"<loc>(.*?)</loc>", text, flags=re.IGNORECASE)
    ]

    year_pages: dict[int, set[str]] = {}
    for link in raw_links:
        if "/books/neet-previous-year-paper/" not in link.lower():
            continue

        year_match = re.search(r"/year-wise/(\d{4})/", link)
        if not year_match:
            continue

        year = int(year_match.group(1))
        if not (start_year <= year <= end_year):
            continue

        year_pages.setdefault(year, set()).add(link)

    return {year: sorted(pages) for year, pages in year_pages.items()}


def extract_selfstudys_pdf_links(session: requests.Session, page_url: str) -> list[str]:
    try:
        response = session.get(page_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        return []

    text = response.text

    links: set[str] = set()
    for match in re.finditer(r"downloadFile\((['\"])(.*?)\1\)", text):
        url = match.group(2).strip()
        if url.startswith("http"):
            links.add(url)

    for match in re.finditer(r"https://www\.selfstudys\.com/sitepdfs/[A-Za-z0-9]+", text):
        links.add(match.group(0).strip())

    return sorted(links)


def score_selfstudys_candidate(page_url: str, pdf_url: str, year: int) -> int:
    score = domain_score(pdf_url) + 45

    lower_page = page_url.lower()
    if "question-paper" in lower_page:
        score += 18
    if "aipmt" in lower_page or "neet" in lower_page or "aiims" in lower_page:
        score += 10
    if re.search(r"/neet-code-[a-z0-9-]+", lower_page):
        score += 8
    if str(year) in lower_page:
        score += 6
    if "answer-key" in lower_page:
        score -= 4

    if "/sitepdfs/" in pdf_url.lower():
        score += 15

    return score


def discover_selfstudys_candidates(
    session: requests.Session,
    year_pages: dict[int, list[str]],
    max_source_pages_per_year: int,
) -> dict[int, list[CandidateLink]]:
    result: dict[int, list[CandidateLink]] = {}

    for year, pages in sorted(year_pages.items()):
        candidates: list[CandidateLink] = []
        seen_urls: set[str] = set()

        for page_url in pages[:max_source_pages_per_year]:
            pdf_links = extract_selfstudys_pdf_links(session, page_url)
            for pdf_url in pdf_links:
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                score = score_selfstudys_candidate(page_url, pdf_url, year)
                candidates.append(
                    CandidateLink(
                        year=year,
                        query="selfstudys_yearwise",
                        source_url=page_url,
                        pdf_url=pdf_url,
                        score=score,
                        domain=urlparse(pdf_url).netloc.lower(),
                    )
                )

            time.sleep(0.1)

        candidates.sort(key=lambda item: item.score, reverse=True)
        result[year] = candidates

    return result


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value)
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    return cleaned or "paper"


def is_pdf_payload(payload: bytes) -> bool:
    return payload[:4] == b"%PDF"


def download_pdf(session: requests.Session, url: str, output_path: Path) -> tuple[bool, str]:
    try:
        response = session.get(url, timeout=40)
    except requests.RequestException as exc:
        return False, f"request_error:{exc.__class__.__name__}"

    if response.status_code != 200:
        return False, f"status_{response.status_code}"

    data = response.content
    if not is_pdf_payload(data):
        return False, "not_pdf"

    output_path.write_bytes(data)
    return True, "ok"


def select_top_candidates(candidates: Iterable[CandidateLink], max_per_year: int) -> list[CandidateLink]:
    candidate_list = list(candidates)
    selected: list[CandidateLink] = []
    used_domains: set[str] = set()

    if not candidate_list:
        return selected

    distinct_domains = {item.domain.replace("www.", "") for item in candidate_list}
    diversify_domains = len(distinct_domains) > 1

    for candidate in candidate_list:
        if len(selected) >= max_per_year:
            break

        domain_key = candidate.domain.replace("www.", "")
        if diversify_domains and domain_key in used_domains and max_per_year > 1:
            continue

        selected.append(candidate)
        used_domains.add(domain_key)

    return selected


def run_scrape(
    start_year: int,
    end_year: int,
    output_dir: Path,
    max_per_year: int,
    search_limit: int,
    page_pdf_probe_limit: int,
    skip_existing: bool,
) -> dict:
    session = build_session()
    papers_dir = output_dir / "papers"
    papers_dir.mkdir(parents=True, exist_ok=True)

    year_pages = discover_selfstudys_year_pages(session, start_year=start_year, end_year=end_year)
    selfstudys_candidates = discover_selfstudys_candidates(
        session,
        year_pages=year_pages,
        max_source_pages_per_year=400,
    )

    manifest_rows: list[dict] = []

    for year in range(start_year, end_year + 1):
        print(f"[INFO] Discovering year {year}...")
        candidates = selfstudys_candidates.get(year, [])

        if not candidates:
            candidates = discover_candidates_for_year(
                session=session,
                year=year,
                search_limit=search_limit,
                page_pdf_probe_limit=page_pdf_probe_limit,
            )

        selected = select_top_candidates(candidates, max_per_year=max_per_year)
        if not selected:
            manifest_rows.append(
                {
                    "year": year,
                    "pdf_url": "",
                    "source_url": "",
                    "domain": "",
                    "score": 0,
                    "status": "not_found",
                    "file_path": "",
                }
            )
            print(f"[WARN] No candidate found for year {year}")
            continue

        for idx, candidate in enumerate(selected, start=1):
            domain_slug = safe_name(candidate.domain.replace("www.", ""))
            file_name = f"neet_{year}_{idx:02d}_{domain_slug}.pdf"
            output_path = papers_dir / file_name

            if skip_existing and output_path.exists() and output_path.stat().st_size > 1000:
                status = "exists"
                ok = True
            else:
                ok, status = download_pdf(session, candidate.pdf_url, output_path)
                if not ok and output_path.exists():
                    output_path.unlink(missing_ok=True)

            manifest_rows.append(
                {
                    "year": year,
                    "pdf_url": candidate.pdf_url,
                    "source_url": candidate.source_url,
                    "domain": candidate.domain,
                    "score": candidate.score,
                    "status": status,
                    "file_path": str(output_path) if ok else "",
                }
            )
            print(f"[{'OK' if ok else 'ERR'}] {year} -> {status} -> {candidate.pdf_url}")

    downloaded = sum(1 for row in manifest_rows if row["status"] in {"ok", "exists"})
    year_covered = len({int(row["year"]) for row in manifest_rows if row["status"] in {"ok", "exists"}})

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "start_year": start_year,
        "end_year": end_year,
        "max_per_year": max_per_year,
        "downloaded_files": downloaded,
        "years_with_at_least_one_paper": year_covered,
        "manifest_rows": len(manifest_rows),
        "papers_dir": str(papers_dir),
    }

    manifest_json = output_dir / "manifest.json"
    manifest_csv = output_dir / "manifest.csv"
    summary_json = output_dir / "summary.json"

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_json.write_text(json.dumps(manifest_rows, indent=2), encoding="utf-8")

    with manifest_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["year", "pdf_url", "source_url", "domain", "score", "status", "file_path"],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)

    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    current_year = datetime.now().year
    default_start = current_year - 19
    parser = argparse.ArgumentParser(description="Scrape NEET/AIPMT papers for a year range.")
    parser.add_argument("--start-year", type=int, default=default_start)
    parser.add_argument("--end-year", type=int, default=current_year)
    parser.add_argument("--max-per-year", type=int, default=1)
    parser.add_argument("--search-limit", type=int, default=18)
    parser.add_argument("--page-pdf-probe-limit", type=int, default=8)
    parser.add_argument("--output-dir", type=Path, default=Path("data") / "neet_papers")
    parser.add_argument("--no-skip-existing", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.end_year < args.start_year:
        raise SystemExit("end-year must be >= start-year")

    summary = run_scrape(
        start_year=args.start_year,
        end_year=args.end_year,
        output_dir=args.output_dir,
        max_per_year=max(1, args.max_per_year),
        search_limit=max(5, args.search_limit),
        page_pdf_probe_limit=max(0, args.page_pdf_probe_limit),
        skip_existing=not args.no_skip_existing,
    )

    print("\n[SUMMARY]")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
