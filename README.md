# NEET Unified Study App (FastAPI + Streamlit)

A modern NEET prep platform with a FastAPI backend and a Streamlit frontend.

A Vite + React frontend can still be used if needed, but `app.py` is the quickest full-feature UI for local use.

## What this app includes

- Exam simulator (custom length, timer, review flow)
- Exam simulator now defaults to 180Q and supports review flags/jump-to-flagged flow
- Adaptive mock engine (weak-topic + dynamic difficulty mix)
- OMR-like practice mode (bubble sheet, section locks, timer)
- Past Year Question mode (year/chapter trend + PYQ quiz)
- Searchable question bank + quick practice logging (subject/topic/question-type/difficulty filters)
- Topic coverage analyzer with chapter + NCERT class mapping
- Topic mastery heatmap (subject-topic mastery scoring from attempts + speed)
- Time analytics per question/topic/subject
- Weakness tracker with mistake-type breakdown
- Mistake Journal 2.0 (root-cause summaries + weak-topic mistake logs)
- Revision scheduler with calendar and completion tracking
- Smart flashcards from wrong attempts (adaptive intervals + leech detection)
- Daily quiz mode with streak tracking + Telegram/WhatsApp share payload builder
- Performance forecast (score range + confidence + target calculator)
- Goal tracker + target date planner with weekly milestones
- Rank projection module (score to estimated rank/percentile)
- Question of the Day workflow (daily question + submission logging)
- Mock question paper builder for generating custom test papers from filters/sections
- Multi-profile coaching dashboard
- Data lab to parse PDF, import answer key, rebuild metadata, and import PYQ tags
- Bookmarks + CSV export for question bank and session reports

## Setup

1. Create or activate your virtual environment.
2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Start the FastAPI backend:

```powershell
uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

4. In a second terminal, start the Streamlit frontend (recommended):

```powershell
.\.venv\Scripts\python.exe scripts\run_streamlit.py
```

Open `http://127.0.0.1:8501`.

Optional: start the Vite frontend instead:

```powershell
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173`.

## Deploy To Streamlit Community Cloud

This project is now cloud-ready with a single app entrypoint.

- Main file path: `app.py`
- Python dependencies: `requirements.txt`
- Runtime behavior: Streamlit auto-starts the local FastAPI backend when API URL is local (`http://127.0.0.1:8000`)

### Streamlit Cloud setup

1. Push this repository to GitHub.
2. In Streamlit Community Cloud, create a new app from this repo.
3. Set **Main file path** to `app.py`.
4. Add secrets if you want AI features:

```toml
GROQ_API_KEY = "your_key_here"
```

5. Deploy.

If you host backend separately, set `API_BASE_URL` in Streamlit Cloud app settings/secrets and the app will use that URL instead of local auto-start.

## FastAPI Routes

- `GET /health` -> service and DB health
- `GET /api/overview` -> totals, tag coverage, subject/difficulty distribution
- `GET /api/tagging-progress` -> tagged vs pending plus confidence bands
- `GET /api/topics` -> topic list and per-topic counts
- `GET /api/questions` -> searchable/filterable paginated question list
- `GET /api/questions/{question_id}` -> full row + latest answer key entry
- `GET /api/daily/share-payload` -> preformatted daily-quiz message for WhatsApp/Telegram share
- `GET /api/mistakes/journal` -> mistake journal rows + root-cause summaries
- `GET /api/analytics/mastery-heatmap` -> topic mastery matrix for subject/topic cells
- `GET /api/goals/current` + `POST /api/goals/set` -> target score planner and milestones
- `GET /api/analytics/rank-projection` -> estimated NEET rank/percentile from projected score
- `GET /api/qotd` + `POST /api/qotd/submit` -> Question of the Day workflow
- `POST /api/mock-paper/build`, `GET /api/mock-paper/list`, `GET /api/mock-paper/{paper_id}` -> mock test paper builder

## Fast Batch Retagging (High Throughput)

Use the new batched retagger to process untagged rows much faster than one-call-per-question flow.

### 1. Install dependency

```powershell
pip install groq
```

### 2. Set Groq API keys (PowerShell)

```powershell
$env:GROQ_KEY_1="gsk_..."
$env:GROQ_KEY_2="gsk_..."
$env:GROQ_KEY_3="gsk_..."
$env:GROQ_KEY_4="gsk_..."
$env:GROQ_KEY_5="gsk_..."
```

Supported env names:

- `GROQ_API_KEY`
- `GROQ_KEY_1`
- `GROQ_KEY_2`
- `GROQ_KEY_3`
- `GROQ_KEY_4`
- `GROQ_KEY_5`

### 3. Smoke test on 30 rows

```powershell
.\.venv\Scripts\python.exe tools\retag_fast.py --db data/db/questions.db --limit 30
```

### 4. Full run

```powershell
.\.venv\Scripts\python.exe tools\retag_fast.py --db data/db/questions.db --workers 8
```

Notes:

- Script path: `tools/retag_fast.py`
- Default model: `llama-3.1-8b-instant`
- Batches 10 questions per request and rotates keys across workers.
- Writes to SQLite in periodic commits (not one huge transaction).
- If a batch response fails validation, it automatically falls back to per-question tagging for that batch.

### Streamlit UI launch options

Recommended Windows-safe launcher:

```powershell
.\.venv\Scripts\python.exe scripts\run_streamlit.py
```

Direct Streamlit command (works on most non-Windows setups):

```powershell
streamlit run app.py
```

## Verification Layer (Project-Wide)

The app now includes a full verification layer to validate:

- Python syntax for core scripts
- Installed dependencies from `requirements.txt`
- Question bank integrity (`data/questions.json`)
- State integrity (`data/study_state.json`)
- Scraped paper artifacts (`data/neet_papers/*`), including PDF header checks
- Optional remote source metadata validation for answer-key/solution pages

### In Streamlit

- Turn on **Show advanced tools (System Check)**.
- Open the **System Check** tab.
- Use **Run Full Verification** for a full report.
- Download the JSON report for audit/history.

### From CLI

```powershell
python scripts/verify_project.py
```

Useful options:

```powershell
python scripts/verify_project.py --deep-pdf-scan --output-json data/verification_report.json
python scripts/verify_project.py --verify-remote-sources --remote-sample-limit 25
```

## Groq AI Integration

The app now includes an **AI Help** tab powered by Groq.

Configure `GROQ_API_KEY` in the **backend terminal** (the one running FastAPI).

### Option A: Environment variable (recommended)

```powershell
$env:GROQ_API_KEY="your_key_here"
uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

### Option B: Persistent shell profile

Add this env var in your shell profile so it loads automatically before starting backend.

Then restart FastAPI.

You can then use:

- AI explanation for selected NEET questions
- AI mistake diagnosis for wrong attempts
- AI-generated similar questions (difficulty ladder)
- Context-aware tutor chat based on your weak areas
- Personalized AI-generated 7-day study plan

## Using your PDF

1. Put your NEET PDF in project root.
2. Open the app and go to **Data Lab**.
3. Choose the PDF and click **Parse selected PDF and overwrite question bank**.
4. Optionally upload answer key CSV for automatic scoring.

## Answer key CSV format

```csv
question_id,answer
GT-07-PHY-001-00001,3
GT-07-PHY-002-00002,1
```

`answer` must be 1, 2, 3, or 4.

## PYQ metadata CSV format

```csv
question_id,year,is_pyq,chapter,ncert_class,difficulty
GT-07-PHY-001-00001,2022,true,Units and Measurements,Class 11,Medium
```

Only `question_id` is mandatory. Other columns are optional but recommended.

## Data files

The app stores local data in:

- `data/questions.json`
- `data/study_state.json`

No cloud/database setup is required for the initial version.

## Scrape Past 20 Years Papers

Use the built-in scraper:

```powershell
.\.venv\Scripts\python.exe scripts\scrape_neet_papers.py --start-year 2006 --end-year 2025 --max-per-year 4 --output-dir data\neet_papers
```

Outputs:

- `data/neet_papers/papers/` -> downloaded PDFs
- `data/neet_papers/manifest.json` -> row-level scrape results
- `data/neet_papers/manifest.csv` -> spreadsheet-friendly results
- `data/neet_papers/summary.json` -> summary stats

Notes:

- `max-per-year` controls how many papers are downloaded for each year.
- Re-running the command skips already downloaded files by default.
