from __future__ import annotations

import json
import re
from statistics import mean, pstdev
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
import unicodedata

DATA_DIR = Path("data")
QUESTIONS_PATH = DATA_DIR / "questions.json"
STATE_PATH = DATA_DIR / "study_state.json"


SAMPLE_QUESTIONS: list[dict[str, Any]] = [
    {
        "id": "SAMPLE-PHY-001",
        "exam_id": "SAMPLE",
        "subject": "PHYSICS",
        "qno": 1,
        "topic": "Mechanics",
        "text": "A body starts from rest and moves with uniform acceleration 2 m/s^2. What is the displacement in 5 s?",
        "options": ["10 m", "25 m", "50 m", "100 m"],
    },
    {
        "id": "SAMPLE-CHE-046",
        "exam_id": "SAMPLE",
        "subject": "CHEMISTRY",
        "qno": 46,
        "topic": "Organic Chemistry",
        "text": "Which functional group is present in ethanol?",
        "options": ["Aldehyde", "Ketone", "Alcohol", "Carboxylic acid"],
    },
    {
        "id": "SAMPLE-BIO-091",
        "exam_id": "SAMPLE",
        "subject": "BIOLOGY",
        "qno": 91,
        "topic": "Genetics",
        "text": "Mendel studied inheritance in which plant?",
        "options": ["Wheat", "Pea", "Rice", "Maize"],
    },
]


TOPIC_KEYWORDS: dict[str, dict[str, list[str]]] = {
    "PHYSICS": {
        "Mechanics": ["velocity", "acceleration", "force", "work", "energy", "collision", "rope", "projectile"],
        "Electromagnetism": ["magnetic", "electric", "solenoid", "inductor", "capacitor", "lcr", "field"],
        "Modern Physics": ["hydrogen", "photoelectric", "nuclear", "de broglie", "atom", "lyman", "balmer"],
        "Waves and Optics": ["wave", "interference", "diffraction", "polarization", "lens", "mirror", "optics"],
    },
    "CHEMISTRY": {
        "Physical Chemistry": ["mole", "equilibrium", "thermodynamics", "electrochem", "kinetics", "enthalpy", "ph"],
        "Organic Chemistry": ["haloalkane", "aldehyde", "ketone", "alcohol", "benzene", "reaction", "hydrocarbon"],
        "Inorganic Chemistry": ["coordination", "complex", "periodic", "metal", "ligand", "salt", "oxidation"],
    },
    "BIOLOGY": {
        "Human Physiology": ["kidney", "hormone", "menstrual", "blood", "lung", "heart", "excretion", "respiration"],
        "Genetics": ["chromosome", "inheritance", "dna", "rna", "mutation", "mendel", "gene"],
        "Ecology and Evolution": ["population", "evolution", "selection", "species", "ecosystem", "darwin"],
        "Plant Biology": ["photosynthesis", "sporopollenin", "taxonomy", "root", "xylem", "phloem"],
    },
    "BOTANY": {
        "Plant Biology": ["photosynthesis", "sporopollenin", "taxonomy", "root", "xylem", "phloem", "stomata"],
    },
    "ZOOLOGY": {
        "Human Physiology": ["hormone", "blood", "heart", "kidney", "respiration", "digestive"],
        "Ecology and Evolution": ["evolution", "population", "species", "selection", "adaptation"],
    },
}


CHAPTER_KEYWORDS: dict[str, list[dict[str, Any]]] = {
    "PHYSICS": [
        {"chapter": "Units and Measurements", "ncert_class": 11, "keywords": ["unit", "dimension", "error", "measurement"]},
        {"chapter": "Laws of Motion and Work", "ncert_class": 11, "keywords": ["force", "friction", "work", "energy", "power", "newton"]},
        {"chapter": "Oscillations and Waves", "ncert_class": 11, "keywords": ["shm", "oscillation", "wave", "frequency", "amplitude"]},
        {"chapter": "Electrostatics and Current", "ncert_class": 12, "keywords": ["charge", "potential", "current", "resistance", "capacitor", "electric field"]},
        {"chapter": "Magnetism and EMI", "ncert_class": 12, "keywords": ["magnetic", "solenoid", "inductor", "flux", "lenz", "faraday", "lcr"]},
        {"chapter": "Ray and Wave Optics", "ncert_class": 12, "keywords": ["lens", "mirror", "optics", "interference", "diffraction", "polarization"]},
        {"chapter": "Modern Physics", "ncert_class": 12, "keywords": ["hydrogen", "photoelectric", "de broglie", "nuclear", "lyman", "balmer", "atom"]},
    ],
    "CHEMISTRY": [
        {"chapter": "Mole Concept and Stoichiometry", "ncert_class": 11, "keywords": ["mole", "stoichiometry", "limiting", "molar"]},
        {"chapter": "Thermodynamics and Equilibrium", "ncert_class": 11, "keywords": ["enthalpy", "gibbs", "equilibrium", "kp", "kc", "entropy"]},
        {"chapter": "Electrochemistry and Kinetics", "ncert_class": 12, "keywords": ["electrochem", "cell", "electrode", "rate", "kinetics", "arrhenius"]},
        {"chapter": "Organic Reaction Basics", "ncert_class": 11, "keywords": ["reaction", "mechanism", "reagent", "sn1", "sn2", "elimination"]},
        {"chapter": "Haloalkanes, Alcohols, Carbonyl", "ncert_class": 12, "keywords": ["haloalkane", "alcohol", "aldehyde", "ketone", "carboxylic"]},
        {"chapter": "Coordination and d-f Block", "ncert_class": 12, "keywords": ["coordination", "ligand", "complex", "crystal field", "d-block"]},
    ],
    "BIOLOGY": [
        {"chapter": "Cell and Biomolecules", "ncert_class": 11, "keywords": ["cell", "organelle", "mitochondria", "enzyme", "protein", "biomolecule"]},
        {"chapter": "Plant Physiology", "ncert_class": 11, "keywords": ["photosynthesis", "respiration", "xylem", "phloem", "stomata", "root"]},
        {"chapter": "Human Physiology", "ncert_class": 11, "keywords": ["kidney", "hormone", "blood", "heart", "lung", "menstrual", "excretion"]},
        {"chapter": "Genetics and Evolution", "ncert_class": 12, "keywords": ["dna", "rna", "gene", "chromosome", "inheritance", "evolution", "darwin"]},
        {"chapter": "Ecology", "ncert_class": 12, "keywords": ["population", "ecosystem", "biodiversity", "food chain", "selection"]},
        {"chapter": "Reproduction", "ncert_class": 12, "keywords": ["gamete", "spermatogenesis", "ovulation", "fertilization", "embryo", "placenta"]},
    ],
    "BOTANY": [
        {"chapter": "Plant Physiology", "ncert_class": 11, "keywords": ["photosynthesis", "xylem", "phloem", "stomata", "root"]},
        {"chapter": "Plant Reproduction", "ncert_class": 12, "keywords": ["pollen", "sporopollenin", "embryo sac", "double fertilization"]},
    ],
    "ZOOLOGY": [
        {"chapter": "Human Physiology", "ncert_class": 11, "keywords": ["kidney", "heart", "blood", "hormone", "respiration"]},
        {"chapter": "Evolution and Ecology", "ncert_class": 12, "keywords": ["evolution", "selection", "population", "species"]},
    ],
}


def infer_chapter_ncert(subject: str, text: str, topic: str = "General") -> tuple[str, str]:
    subject_key = (subject or "").upper()
    text_l = text.lower()
    chapter_rows = CHAPTER_KEYWORDS.get(subject_key, [])

    best_chapter = topic if topic and topic != "General" else "General"
    best_class = "Unknown"
    best_score = 0

    for row in chapter_rows:
        score = sum(1 for keyword in row.get("keywords", []) if keyword in text_l)
        if score > best_score:
            best_score = score
            best_chapter = row.get("chapter", best_chapter)
            best_class = f"Class {row.get('ncert_class', 'Unknown')}"

    return best_chapter, best_class


def infer_difficulty(text: str) -> str:
    text_l = text.lower()
    hard_markers = ["assertion", "reason", "match", "statement", "derive", "calculate", "proof"]
    medium_markers = ["concept", "identify", "correct", "incorrect", "relation"]

    hard_hits = sum(1 for marker in hard_markers if marker in text_l)
    medium_hits = sum(1 for marker in medium_markers if marker in text_l)
    char_len = len(text)

    if hard_hits >= 2 or char_len > 220:
        return "Hard"
    if hard_hits >= 1 or medium_hits >= 1 or char_len > 130:
        return "Medium"
    return "Easy"


def infer_year(exam_id: str, text: str) -> int | None:
    combined = f"{exam_id} {text}"
    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", combined)
    if not year_match:
        return None
    year = int(year_match.group(1))
    if 1990 <= year <= (date.today().year + 1):
        return year
    return None


def enrich_question_metadata(question: dict[str, Any]) -> dict[str, Any]:
    updated = dict(question)

    subject = str(updated.get("subject", "UNKNOWN"))
    question_text = clean_ocr_text(str(updated.get("text", "")))
    updated["text"] = question_text

    raw_options = updated.get("options", [])
    if not isinstance(raw_options, list):
        raw_options = []
    raw_options = raw_options[:4]
    while len(raw_options) < 4:
        raw_options.append("")

    cleaned_options: list[str] = []
    option_scores: list[int] = []
    placeholder_count = 0
    good_option_count = 0
    cross_question_leak = False
    for idx, option in enumerate(raw_options, start=1):
        cleaned = clean_ocr_text(str(option))
        score = text_quality_score(cleaned)
        option_scores.append(score)

        if re.fullmatch(r"Option\s+[1-4]", cleaned, flags=re.IGNORECASE):
            placeholder_count += 1

        if re.search(r"\b\d{1,3}\s*[.)]\s+[A-Za-z]", cleaned):
            cross_question_leak = True

        word_count = len(re.findall(r"\b[A-Za-z]{2,}\b", cleaned))
        if score >= 45 and word_count >= 2 and not re.fullmatch(r"Option\s+[1-4]", cleaned, flags=re.IGNORECASE):
            good_option_count += 1

        if score < 30:
            cleaned = f"Option {idx}"
        cleaned_options.append(cleaned)
    updated["options"] = cleaned_options

    topic = str(updated.get("topic", "General"))

    if not topic or topic == "General":
        topic = infer_topic(subject, question_text)
        updated["topic"] = topic

    chapter, ncert_class = infer_chapter_ncert(subject, question_text, topic)
    updated.setdefault("chapter", chapter)
    updated.setdefault("ncert_class", ncert_class)

    difficulty = infer_difficulty(question_text)
    updated.setdefault("difficulty", difficulty)

    year = infer_year(str(updated.get("exam_id", "")), question_text)
    if "year" not in updated:
        updated["year"] = year

    if "is_pyq" not in updated:
        updated["is_pyq"] = bool(year is not None)

    question_score = text_quality_score(question_text)
    avg_option_score = round(float(mean(option_scores)), 2) if option_scores else 0.0
    updated["quality_score"] = round((question_score * 0.7) + (avg_option_score * 0.3), 2)
    updated["placeholder_option_count"] = placeholder_count
    updated["good_option_count"] = good_option_count
    updated["cross_question_leak"] = cross_question_leak

    updated["is_usable"] = bool(
        question_score >= 55
        and avg_option_score >= 45
        and good_option_count >= 3
        and placeholder_count == 0
        and not cross_question_leak
    )

    return updated


def enrich_questions_metadata(questions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [enrich_question_metadata(question) for question in questions]


def classify_mistake_type(
    question_text: str,
    selected_option: int | None,
    correct_option: int | None,
    time_spent_sec: int | None,
) -> str:
    if selected_option is None:
        return "Skipped"

    if correct_option is None:
        return "Ungraded"

    if int(selected_option) == int(correct_option):
        return "No Mistake"

    text_l = question_text.lower()
    conceptual_markers = ["assertion", "reason", "statement", "which of the following", "match"]
    formula_markers = ["calculate", "find", "numerical", "velocity", "current", "mole", "enthalpy"]

    if time_spent_sec is not None and time_spent_sec < 25:
        return "Silly Mistake"
    if any(marker in text_l for marker in formula_markers):
        return "Formula Gap"
    if any(marker in text_l for marker in conceptual_markers):
        return "Concept Gap"
    if time_spent_sec is not None and time_spent_sec > 130:
        return "Time Pressure Mistake"
    return "Concept Gap"


def generate_revision_calendar(
    weak_topics: list[str],
    start_date: date,
    end_date: date,
    daily_question_target: int = 50,
) -> list[dict[str, Any]]:
    if end_date < start_date:
        end_date = start_date

    topic_cycle = weak_topics[:] if weak_topics else ["General Revision"]
    calendar_rows: list[dict[str, Any]] = []
    cursor = start_date
    idx = 0

    while cursor <= end_date:
        topic = topic_cycle[idx % len(topic_cycle)]
        calendar_rows.append(
            {
                "date": cursor.isoformat(),
                "topic": topic,
                "tasks": [
                    f"45 min concept revision for {topic}",
                    f"Solve {daily_question_target} MCQs",
                    "15 min error-log review",
                ],
                "completed": False,
            }
        )
        cursor += timedelta(days=1)
        idx += 1

    return calendar_rows


def performance_forecast(exam_history: list[dict[str, Any]]) -> dict[str, Any]:
    if not exam_history:
        return {
            "predicted_score": 0,
            "low": 0,
            "high": 0,
            "confidence": "Low",
            "recommended_accuracy": 75.0,
        }

    recent = exam_history[-12:]
    scores = [float(item.get("score", 0)) for item in recent]
    graded = [int(item.get("graded", 0)) for item in recent]
    correct = [int(item.get("correct", 0)) for item in recent]

    expected_score = mean(scores)
    spread = pstdev(scores) if len(scores) > 1 else 20.0
    low = max(0, round(expected_score - spread))
    high = min(720, round(expected_score + spread))

    total_graded = sum(graded)
    total_correct = sum(correct)
    current_accuracy = (total_correct / total_graded) * 100 if total_graded else 0.0

    if len(scores) >= 8:
        confidence = "High"
    elif len(scores) >= 4:
        confidence = "Medium"
    else:
        confidence = "Low"

    recommended_accuracy = max(55.0, min(95.0, 65.0 + (500 - expected_score) / 8))

    return {
        "predicted_score": round(expected_score),
        "low": low,
        "high": high,
        "confidence": confidence,
        "current_accuracy": round(current_accuracy, 2),
        "recommended_accuracy": round(recommended_accuracy, 2),
    }


def ensure_data_files() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not QUESTIONS_PATH.exists():
        QUESTIONS_PATH.write_text(json.dumps(SAMPLE_QUESTIONS, indent=2), encoding="utf-8")
    if not STATE_PATH.exists():
        default_state = {
            "users": {},
            "answer_key": {},
            "meta": {"created_at": datetime.now().isoformat(timespec="seconds")},
        }
        STATE_PATH.write_text(json.dumps(default_state, indent=2), encoding="utf-8")


def load_questions() -> list[dict[str, Any]]:
    ensure_data_files()
    try:
        questions = json.loads(QUESTIONS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        questions = SAMPLE_QUESTIONS
    if not isinstance(questions, list) or not questions:
        return SAMPLE_QUESTIONS

    enriched = enrich_questions_metadata(questions)
    if enriched != questions:
        save_questions(enriched)
    return enriched


def save_questions(questions: list[dict[str, Any]]) -> None:
    ensure_data_files()
    QUESTIONS_PATH.write_text(json.dumps(questions, indent=2), encoding="utf-8")


def load_state() -> dict[str, Any]:
    ensure_data_files()
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        state = {"users": {}, "answer_key": {}, "meta": {}}

    state.setdefault("users", {})
    state.setdefault("answer_key", {})
    state.setdefault("meta", {})
    return state


def save_state(state: dict[str, Any]) -> None:
    ensure_data_files()
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def ensure_user(state: dict[str, Any], username: str) -> dict[str, Any]:
    users = state.setdefault("users", {})
    if username not in users:
        users[username] = {
            "attempts": [],
            "exam_history": [],
            "daily_quiz_history": [],
            "flashcards": [],
            "mistake_diagnostics": {},
            "revision_plan": [],
            "omr_history": [],
            "pyq_history": [],
            "ai_generated_sets": [],
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }
    user_data = users[username]
    user_data.setdefault("attempts", [])
    user_data.setdefault("exam_history", [])
    user_data.setdefault("daily_quiz_history", [])
    user_data.setdefault("flashcards", [])
    user_data.setdefault("mistake_diagnostics", {})
    user_data.setdefault("revision_plan", [])
    user_data.setdefault("omr_history", [])
    user_data.setdefault("pyq_history", [])
    user_data.setdefault("ai_generated_sets", [])
    return user_data


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def clean_ocr_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    replacement_map = {
        "→": "->",
        "➔": "->",
        "•": " ",
        "✓": " ",
        "~": " ",
        "`": " ",
        "_": " ",
        "|": " ",
    }
    for source, target in replacement_map.items():
        text = text.replace(source, target)

    text = re.sub(r"[^A-Za-z0-9\s\.,;:!\?\(\)\[\]\{\}\+\-\*\/%=<>°'\"#&]", " ", text)
    text = re.sub(r"([,.;:!\?\-])\1{1,}", r"\1", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def text_quality_score(value: str) -> int:
    text = str(value or "").strip()
    if not text:
        return 0

    length = len(text)
    letters = len(re.findall(r"[A-Za-z]", text))
    words = len(re.findall(r"\b[A-Za-z]{2,}\b", text))
    alnum_space = sum(1 for char in text if char.isalnum() or char.isspace())
    ratio = alnum_space / max(1, length)

    score = 100
    if length < 20:
        score -= 40
    elif length < 45:
        score -= 20

    if words < 4:
        score -= 35
    elif words < 7:
        score -= 20

    if letters < 15:
        score -= 20

    if ratio < 0.7:
        score -= 30
    elif ratio < 0.8:
        score -= 15

    if re.search(r"(?:\b[A-Za-z]\b\s*){5,}", text):
        score -= 20

    return max(0, min(100, score))


def infer_topic(subject: str, text: str) -> str:
    subject_key = (subject or "").upper()
    text_l = text.lower()
    topic_map = TOPIC_KEYWORDS.get(subject_key, {})

    best_topic = "General"
    best_score = 0
    for topic, keywords in topic_map.items():
        score = sum(1 for keyword in keywords if keyword in text_l)
        if score > best_score:
            best_score = score
            best_topic = topic
    return best_topic


def _subject_by_qno(qno: int) -> str:
    if 1 <= qno <= 45:
        return "PHYSICS"
    if 46 <= qno <= 90:
        return "CHEMISTRY"
    return "BIOLOGY"


def _context_from_markers(markers: list[tuple[int, str]], position: int, default_value: str) -> str:
    current = default_value
    for marker_pos, marker_value in markers:
        if marker_pos > position:
            break
        current = marker_value
    return current


def _is_noise_line(line: str) -> bool:
    return bool(
        re.search(
            r"resonance|educational institutions|page\s*\d+|mp\+mr|paper setter|verifier",
            line,
            re.IGNORECASE,
        )
    )


def _parse_question_chunk(chunk: str, qno: int) -> tuple[str, list[str]]:
    compact_chunk = normalize_text(clean_ocr_text(chunk))
    compact_chunk = re.sub(r"^\s*\d{1,3}\s*[.)]\s*", "", compact_chunk)

    # First attempt: parse inline options from explicit markers (1)-(4), which is more robust
    # for OCR outputs where options may collapse onto one line.
    marker_matches = list(re.finditer(r"\(\s*([1-4])\s*\)", compact_chunk))
    if marker_matches:
        first_marker = marker_matches[0].start()
        stem_candidate = normalize_text(compact_chunk[:first_marker])
        marker_option_map: dict[int, str] = {}

        for idx, marker in enumerate(marker_matches):
            option_number = int(marker.group(1))
            option_start = marker.end()
            option_end = marker_matches[idx + 1].start() if idx + 1 < len(marker_matches) else len(compact_chunk)
            option_text = normalize_text(compact_chunk[option_start:option_end])
            option_text = re.split(r"\b\d{1,3}\s*[.)]\s+[A-Za-z]", option_text)[0].strip()

            previous = marker_option_map.get(option_number, "")
            if len(option_text) > len(previous):
                marker_option_map[option_number] = option_text

        marker_options = [clean_ocr_text(marker_option_map.get(index, "")) for index in (1, 2, 3, 4)]
        non_empty_marker_options = sum(1 for option in marker_options if option)
        if stem_candidate and non_empty_marker_options >= 2:
            return stem_candidate, marker_options

    # Fallback: line-based parsing when marker parsing is insufficient.
    lines = [normalize_text(clean_ocr_text(line)) for line in chunk.splitlines()]
    lines = [line for line in lines if line]
    if not lines:
        return "", []

    lines[0] = re.sub(r"^\s*\d{1,3}\s*[.)]\s*", "", lines[0]).strip()

    stem_parts: list[str] = []
    option_map: dict[int, str] = {}
    current_option: int | None = None

    for line in lines:
        if not line or _is_noise_line(line):
            continue

        option_match = re.match(r"^\(\s*([1-4])\s*\)\s*(.*)$", line)
        if option_match:
            current_option = int(option_match.group(1))
            option_map[current_option] = option_match.group(2).strip()
            continue

        if current_option is None:
            stem_parts.append(line)
        else:
            clipped = re.split(r"\b\d{1,3}\s*[.)]\s+[A-Za-z]", line)[0].strip()
            option_map[current_option] = normalize_text(f"{option_map[current_option]} {clipped}")

    question_text = normalize_text(" ".join(stem_parts))
    if not question_text:
        question_text = normalize_text(clean_ocr_text(re.sub(r"^\s*\d{1,3}\s*[.)]\s*", "", chunk[:220])))

    options = [clean_ocr_text(option_map.get(index, "")) for index in (1, 2, 3, 4)]
    if not any(options):
        options = ["", "", "", ""]

    return question_text, options


def parse_questions_from_text(raw_text: str, source_name: str = "source") -> list[dict[str, Any]]:
    text = raw_text.replace("\r", "\n")

    exam_markers = [
        (match.start(), f"GT-{match.group(1)}")
        for match in re.finditer(r"MP\+MR[_-]?GT[-_ ]?(\d{2})", text, flags=re.IGNORECASE)
    ]
    if not exam_markers:
        exam_markers = [(0, Path(source_name).stem.upper())]

    subject_markers = [
        (match.start(), match.group(1).upper())
        for match in re.finditer(r"(?im)^\s*(PHYSICS|CHEMISTRY|BIOLOGY|BOTANY|ZOOLOGY)\b.*$", text)
    ]

    question_starts = list(re.finditer(r"(?m)^\s*(\d{1,3})\s*[.)]\s+", text))

    questions: list[dict[str, Any]] = []
    dedupe: set[tuple[str, str, int, str]] = set()
    id_counter = 1

    for idx, match in enumerate(question_starts):
        qno = int(match.group(1))
        if qno < 1 or qno > 200:
            continue

        chunk_start = match.start()
        chunk_end = question_starts[idx + 1].start() if idx + 1 < len(question_starts) else len(text)
        chunk = text[chunk_start:chunk_end]

        question_text, options = _parse_question_chunk(chunk, qno)
        if len(question_text) < 10:
            continue

        subject = _context_from_markers(subject_markers, chunk_start, _subject_by_qno(qno))
        exam_id = _context_from_markers(exam_markers, chunk_start, "GT-UNKNOWN")
        topic = infer_topic(subject, question_text)

        dedupe_key = (exam_id, subject, qno, question_text[:160].lower())
        if dedupe_key in dedupe:
            continue
        dedupe.add(dedupe_key)

        question_id = f"{exam_id}-{subject[:3]}-{qno:03d}-{id_counter:05d}"
        question_row = {
            "id": question_id,
            "exam_id": exam_id,
            "subject": subject,
            "qno": qno,
            "topic": topic,
            "text": question_text,
            "options": options,
        }
        questions.append(enrich_question_metadata(question_row))
        id_counter += 1

    return questions


def parse_pdf_bytes(pdf_bytes: bytes, source_name: str = "uploaded.pdf") -> list[dict[str, Any]]:
    from pypdf import PdfReader

    reader = PdfReader(BytesIO(pdf_bytes))
    combined_text_parts: list[str] = []
    for page in reader.pages:
        combined_text_parts.append(page.extract_text() or "")
    raw_text = "\n".join(combined_text_parts)
    return parse_questions_from_text(raw_text, source_name=source_name)


def parse_pdf_file(pdf_path: str | Path) -> list[dict[str, Any]]:
    path = Path(pdf_path)
    return parse_pdf_bytes(path.read_bytes(), source_name=path.name)


def record_attempt(
    state: dict[str, Any],
    username: str,
    question: dict[str, Any],
    selected_option: int | None,
    is_correct: bool | None,
    mode: str,
    time_spent_sec: int | None = None,
    mistake_type: str | None = None,
) -> None:
    user = ensure_user(state, username)

    resolved_mistake = mistake_type
    if resolved_mistake is None:
        resolved_mistake = classify_mistake_type(
            question_text=str(question.get("text", "")),
            selected_option=selected_option,
            correct_option=question.get("_key_option"),
            time_spent_sec=time_spent_sec,
        )

    user["attempts"].append(
        {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "question_id": question["id"],
            "exam_id": question.get("exam_id", "UNKNOWN"),
            "subject": question.get("subject", "UNKNOWN"),
            "topic": question.get("topic", "General"),
            "chapter": question.get("chapter", "General"),
            "ncert_class": question.get("ncert_class", "Unknown"),
            "difficulty": question.get("difficulty", "Medium"),
            "selected_option": selected_option,
            "correct": is_correct,
            "mode": mode,
            "time_spent_sec": time_spent_sec,
            "mistake_type": resolved_mistake,
        }
    )


def due_today(date_text: str) -> bool:
    try:
        due_date = date.fromisoformat(date_text)
    except ValueError:
        return True
    return due_date <= date.today()


def schedule_flashcard(card: dict[str, Any], rating: str) -> None:
    rating = rating.lower()
    if rating == "again":
        interval = 1
        ease = max(1.3, float(card.get("ease", 2.3)) - 0.2)
    elif rating == "easy":
        interval = max(4, int(card.get("interval", 1)) * 2)
        ease = float(card.get("ease", 2.3)) + 0.15
    else:
        interval = max(2, int(card.get("interval", 1)) + 2)
        ease = float(card.get("ease", 2.3))

    card["interval"] = interval
    card["ease"] = round(ease, 2)
    card["next_due"] = (date.today() + timedelta(days=interval)).isoformat()
    card["last_reviewed"] = datetime.now().isoformat(timespec="seconds")


def user_metrics(state: dict[str, Any], username: str) -> dict[str, Any]:
    user = ensure_user(state, username)
    attempts = user.get("attempts", [])
    graded = [item for item in attempts if item.get("correct") is not None]
    correct = sum(1 for item in graded if item.get("correct") is True)
    wrong = sum(1 for item in graded if item.get("correct") is False)
    accuracy = round((correct / len(graded)) * 100, 2) if graded else 0.0

    return {
        "attempts": len(attempts),
        "graded_attempts": len(graded),
        "correct": correct,
        "wrong": wrong,
        "accuracy": accuracy,
        "exam_count": len(user.get("exam_history", [])),
    }
