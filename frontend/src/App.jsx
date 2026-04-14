import { useEffect, useMemo, useState } from "react";

const API_BASE = (import.meta.env.VITE_API_BASE || "").replace(/\/$/, "");
const PAGE_SIZE = 12;
const BOOKMARKS_KEY = "neet-bookmarks-v1";

const TABS = [
  { id: "dashboard", label: "Dashboard" },
  { id: "data", label: "Data Lab" },
  { id: "verification", label: "Verification" },
  { id: "exam", label: "Full Test" },
  { id: "adaptive", label: "Adaptive" },
  { id: "omr", label: "OMR" },
  { id: "pyq", label: "PYQ" },
  { id: "bank", label: "Question Bank" },
  { id: "topic", label: "Topic Coverage" },
  { id: "time", label: "Time Analytics" },
  { id: "weak", label: "Weakness" },
  { id: "revision", label: "Revision" },
  { id: "flash", label: "Flashcards" },
  { id: "daily", label: "Daily Quiz" },
  { id: "ai", label: "AI Tutor" },
  { id: "forecast", label: "Forecast" },
  { id: "coaching", label: "Coaching" },
];

const PRACTICE_CONFIG = {
  exam: { title: "Full Test Mode", count: 180, duration: 180, onlyPyq: false },
  adaptive: { title: "Adaptive Mock Engine", count: 60, duration: 120, onlyPyq: false },
  omr: { title: "OMR-Like Practice", count: 90, duration: 180, onlyPyq: false },
  pyq: { title: "Past Year Question Mode", count: 40, duration: 90, onlyPyq: true },
  "daily-quiz": { title: "Daily Quiz Bot", count: 10, duration: 25, onlyPyq: false },
};

function buildUrl(path, params = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    query.set(key, String(value));
  }
  const qs = query.toString();
  return `${API_BASE}${path}${qs ? `?${qs}` : ""}`;
}

async function apiGet(path, params = {}) {
  const res = await fetch(buildUrl(path, params));
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(detail || `Request failed: ${res.status}`);
  }
  return res.json();
}

async function apiPost(path, payload = {}) {
  const res = await fetch(buildUrl(path), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(detail || `Request failed: ${res.status}`);
  }
  return res.json();
}

function toPct(num, den) {
  if (!den) {
    return 0;
  }
  return Math.round((Number(num || 0) / Number(den || 1)) * 10000) / 100;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function formatSeconds(total) {
  const sec = Math.max(0, Math.floor(Number(total || 0)));
  const minutes = Math.floor(sec / 60);
  const seconds = sec % 60;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function csvCell(value) {
  const text = String(value ?? "").replace(/"/g, '""');
  return `"${text}"`;
}

function downloadCsv(filename, columns, rows) {
  if (!Array.isArray(columns) || !columns.length) {
    return;
  }

  const header = columns.map((item) => csvCell(item.label)).join(",");
  const body = (rows || [])
    .map((row) => columns.map((item) => csvCell(row?.[item.key])).join(","))
    .join("\n");

  const blob = new Blob([`${header}\n${body}`], { type: "text/csv;charset=utf-8;" });
  const href = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(href);
}

function DataTable({ columns, rows, emptyText = "No data" }) {
  if (!rows || rows.length === 0) {
    return <p className="muted">{emptyText}</p>;
  }

  return (
    <div className="table-wrap">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.key}>{col.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={row.id || row.key || idx}>
              {columns.map((col) => (
                <td key={col.key}>{col.render ? col.render(row[col.key], row) : row[col.key]}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MetricCard({ label, value, hint }) {
  return (
    <article className="metric-card card-enter">
      <p className="metric-label">{label}</p>
      <h3 className="metric-value">{value}</h3>
      <p className="metric-hint">{hint}</p>
    </article>
  );
}

function SubjectBar({ item, max }) {
  const width = max > 0 ? Math.max(6, Math.round((item.total / max) * 100)) : 0;
  const taggedPct = item.total > 0 ? Math.round((item.tagged / item.total) * 100) : 0;
  return (
    <div className="subject-row">
      <div className="subject-head">
        <span>{item.subject}</span>
        <span>{item.total}</span>
      </div>
      <div className="bar-track">
        <div className="bar-fill" style={{ width: `${width}%` }} />
      </div>
      <small className="subject-tagged">{taggedPct}% tagged</small>
    </div>
  );
}

function TagChip({ text, tone = "default" }) {
  return <span className={`chip chip-${tone}`}>{text}</span>;
}

function DifficultyBadge({ difficulty }) {
  const value = String(difficulty || "unknown").toLowerCase();
  let tone = "neutral";
  if (value === "easy") {
    tone = "good";
  } else if (value === "medium") {
    tone = "warm";
  } else if (value === "hard") {
    tone = "alert";
  }
  return <TagChip text={value} tone={tone} />;
}

export default function App() {
  const [activeTab, setActiveTab] = useState("dashboard");
  const [clockTick, setClockTick] = useState(0);

  const [users, setUsers] = useState(["default"]);
  const [userName, setUserName] = useState("default");
  const [meta, setMeta] = useState({
    subjects: [],
    topics: [],
    source_years: [],
    difficulties: [],
    question_types: [],
  });

  const [overview, setOverview] = useState(null);
  const [taggingProgress, setTaggingProgress] = useState(null);
  const [topicsData, setTopicsData] = useState([]);
  const [dataSummary, setDataSummary] = useState(null);
  const [verificationReport, setVerificationReport] = useState(null);
  const [timeAnalytics, setTimeAnalytics] = useState(null);
  const [weakness, setWeakness] = useState(null);
  const [forecast, setForecast] = useState(null);
  const [coaching, setCoaching] = useState(null);
  const [revisionPlan, setRevisionPlan] = useState(null);
  const [flashcards, setFlashcards] = useState(null);
  const [dailyStreak, setDailyStreak] = useState(0);

  const [error, setError] = useState("");
  const [busyAction, setBusyAction] = useState("");

  const [search, setSearch] = useState("");
  const [subject, setSubject] = useState("");
  const [topic, setTopic] = useState("");
  const [questionType, setQuestionType] = useState("");
  const [difficulty, setDifficulty] = useState("");
  const [onlyTagged, setOnlyTagged] = useState(true);
  const [onlyBookmarked, setOnlyBookmarked] = useState(false);
  const [bookmarks, setBookmarks] = useState([]);
  const [page, setPage] = useState(0);
  const [questions, setQuestions] = useState([]);
  const [totalQuestions, setTotalQuestions] = useState(0);

  const [practiceConfig, setPracticeConfig] = useState(() => ({
    exam: { subject: "", topic: "", questionType: "", difficulty: "", count: 180, duration: 180, searchText: "", onlyTagged: true },
    adaptive: { subject: "", topic: "", questionType: "", difficulty: "", count: 60, duration: 120, searchText: "", onlyTagged: true },
    omr: { subject: "", topic: "", questionType: "", difficulty: "", count: 90, duration: 180, searchText: "", onlyTagged: true },
    pyq: { subject: "", topic: "", questionType: "", difficulty: "", count: 40, duration: 90, searchText: "", onlyTagged: true },
    "daily-quiz": {
      subject: "",
      topic: "",
      questionType: "",
      difficulty: "",
      count: 10,
      duration: 25,
      searchText: "",
      onlyTagged: true,
    },
  }));
  const [practiceSessions, setPracticeSessions] = useState({});

  const [selectedBankQid, setSelectedBankQid] = useState(null);
  const [selectedBankOption, setSelectedBankOption] = useState(0);
  const [selectedBankManualCorrect, setSelectedBankManualCorrect] = useState(false);

  const [revisionDays, setRevisionDays] = useState(60);
  const [revisionTarget, setRevisionTarget] = useState(60);

  const [aiPrompt, setAiPrompt] = useState("");
  const [aiReply, setAiReply] = useState("");
  const [aiExplainQid, setAiExplainQid] = useState("");
  const [aiExplainOption, setAiExplainOption] = useState(0);
  const [aiExplainReply, setAiExplainReply] = useState("");
  const [dailySharePayload, setDailySharePayload] = useState(null);

  const subjectOptions = useMemo(() => {
    return (overview?.by_subject || []).map((item) => item.subject);
  }, [overview]);

  const maxSubjectCount = useMemo(() => {
    const list = overview?.by_subject || [];
    return list.length ? Math.max(...list.map((item) => Number(item.total || 0))) : 0;
  }, [overview]);

  const topicOptions = useMemo(() => {
    return topicsData.map((item) => item.topic);
  }, [topicsData]);

  const questionTypeOptions = useMemo(() => {
    return Array.isArray(meta?.question_types) ? meta.question_types : [];
  }, [meta]);

  const questionMap = useMemo(() => {
    const out = {};
    for (const item of questions) {
      out[String(item.id)] = item;
    }
    return out;
  }, [questions]);

  const visibleQuestions = useMemo(() => {
    if (!onlyBookmarked) {
      return questions;
    }
    const markSet = new Set((bookmarks || []).map((item) => Number(item)));
    return (questions || []).filter((item) => markSet.has(Number(item.id)));
  }, [questions, onlyBookmarked, bookmarks]);

  const bankQuestion = selectedBankQid ? questionMap[String(selectedBankQid)] : null;

  async function refreshOverview() {
    try {
      const [overviewData, progressData] = await Promise.all([
        apiGet("/api/overview"),
        apiGet("/api/tagging-progress"),
      ]);
      setOverview(overviewData);
      setTaggingProgress(progressData);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load overview");
    }
  }

  async function refreshMeta() {
    try {
      const [metaData, usersData] = await Promise.all([apiGet("/api/meta/options"), apiGet("/api/users")]);
      setMeta({
        subjects: metaData?.subjects || [],
        topics: metaData?.topics || [],
        source_years: metaData?.source_years || [],
        difficulties: metaData?.difficulties || [],
        question_types: metaData?.question_types || [],
      });
      const fetchedUsers = usersData?.users?.length ? usersData.users : ["default"];
      setUsers(fetchedUsers);
      if (!fetchedUsers.includes(userName)) {
        setUserName(fetchedUsers[0]);
      }
    } catch (err) {
      setError(err.message || "Failed to load options");
    }
  }

  async function refreshTopics(activeSubject) {
    try {
      const data = await apiGet("/api/topics", {
        subject: activeSubject || undefined,
        limit: 200,
      });
      setTopicsData(data.items || []);
    } catch (err) {
      setError(err.message || "Failed to load topics");
      setTopicsData([]);
    }
  }

  async function refreshQuestions() {
    try {
      const data = await apiGet("/api/questions", {
        q: search || undefined,
        subject: subject || undefined,
        topic: topic || undefined,
        question_type: questionType || undefined,
        difficulty: difficulty || undefined,
        only_tagged: onlyTagged,
        limit: PAGE_SIZE,
        offset: page * PAGE_SIZE,
      });
      setQuestions(data.items || []);
      setTotalQuestions(Number(data.total || 0));
      if (!selectedBankQid && data.items?.length) {
        setSelectedBankQid(data.items[0].id);
      }
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load questions");
    }
  }

  async function refreshDataLab() {
    try {
      const data = await apiGet("/api/data/summary");
      setDataSummary(data);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load data summary");
    }
  }

  async function refreshVerificationSnapshot() {
    try {
      const report = await apiGet("/api/verification/snapshot");
      setVerificationReport(report);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load verification snapshot");
    }
  }

  async function runVerification({ deep = false, remote = false } = {}) {
    setBusyAction("verification");
    try {
      const report = await apiPost("/api/verification/run", {
        deep_pdf_scan: deep,
        verify_remote_sources: remote,
        pdf_sample_limit: 20,
        remote_sample_limit: 20,
        remote_timeout_seconds: 20,
      });
      setVerificationReport(report);
      setError("");
    } catch (err) {
      setError(err.message || "Verification failed");
    } finally {
      setBusyAction("");
    }
  }

  async function refreshTimeAnalytics() {
    try {
      const data = await apiGet("/api/analytics/time", { user_name: userName });
      setTimeAnalytics(data);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load time analytics");
    }
  }

  async function refreshWeakness() {
    try {
      const data = await apiGet("/api/analytics/weakness", { user_name: userName });
      setWeakness(data);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load weakness analytics");
    }
  }

  async function refreshForecast() {
    try {
      const data = await apiGet("/api/analytics/forecast", { user_name: userName });
      setForecast(data);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load forecast");
    }
  }

  async function refreshCoaching() {
    try {
      const data = await apiGet("/api/analytics/coaching");
      setCoaching(data);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load coaching metrics");
    }
  }

  async function refreshRevisionPlan() {
    try {
      const data = await apiGet("/api/revision/plan", { user_name: userName });
      setRevisionPlan(data);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load revision plan");
    }
  }

  async function generateRevisionPlan() {
    setBusyAction("revision-generate");
    try {
      await apiPost("/api/revision/generate", {
        user_name: userName,
        days: revisionDays,
        daily_question_target: revisionTarget,
        weak_topics: (weakness?.items || []).slice(0, 8).map((item) => item.topic),
      });
      await refreshRevisionPlan();
      setError("");
    } catch (err) {
      setError(err.message || "Failed to generate revision plan");
    } finally {
      setBusyAction("");
    }
  }

  async function markRevisionDay(day, completed) {
    setBusyAction(`revision-${day}`);
    try {
      await apiPost("/api/revision/mark", {
        user_name: userName,
        plan_date: day,
        completed,
      });
      await refreshRevisionPlan();
      setError("");
    } catch (err) {
      setError(err.message || "Failed to update revision day");
    } finally {
      setBusyAction("");
    }
  }

  async function refreshFlashcards() {
    try {
      const data = await apiGet("/api/flashcards", {
        user_name: userName,
        due_only: false,
        limit: 200,
      });
      setFlashcards(data);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load flashcards");
    }
  }

  async function generateFlashcards() {
    setBusyAction("flash-generate");
    try {
      await apiPost("/api/flashcards/generate", { user_name: userName, limit: 300 });
      await refreshFlashcards();
      setError("");
    } catch (err) {
      setError(err.message || "Failed to generate flashcards");
    } finally {
      setBusyAction("");
    }
  }

  async function reviewFlashcard(questionId, rating) {
    setBusyAction(`flash-${questionId}`);
    try {
      await apiPost("/api/flashcards/review", {
        user_name: userName,
        question_id: questionId,
        rating,
      });
      await refreshFlashcards();
      setError("");
    } catch (err) {
      setError(err.message || "Failed to review flashcard");
    } finally {
      setBusyAction("");
    }
  }

  async function refreshDailyStreak() {
    try {
      const data = await apiGet("/api/daily/streak", { user_name: userName });
      setDailyStreak(Number(data.streak || 0));
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load daily streak");
    }
  }

  async function refreshDailySharePayload() {
    try {
      const data = await apiGet("/api/daily/share-payload", { user_name: userName });
      setDailySharePayload(data || null);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to load daily share payload");
    }
  }

  async function copyDailyShareMessage() {
    const text = String(dailySharePayload?.message || "").trim();
    if (!text) {
      return;
    }

    try {
      await navigator.clipboard.writeText(text);
      setError("");
    } catch {
      setError("Clipboard access denied. Copy manually from the share preview.");
    }
  }

  function toggleBookmark(questionId) {
    const numericId = Number(questionId || 0);
    if (!numericId) {
      return;
    }
    setBookmarks((prev) => {
      const set = new Set((prev || []).map((item) => Number(item)));
      if (set.has(numericId)) {
        set.delete(numericId);
      } else {
        set.add(numericId);
      }
      return Array.from(set).sort((a, b) => a - b);
    });
  }

  function isBookmarked(questionId) {
    const numericId = Number(questionId || 0);
    if (!numericId) {
      return false;
    }
    return (bookmarks || []).includes(numericId);
  }

  function exportCurrentQuestionsCsv() {
    const stamp = new Date().toISOString().slice(0, 10);
    const rows = (visibleQuestions || []).map((item) => ({
      id: item.id,
      subject: item.subject,
      topic: item.topic,
      question_type: item.question_type,
      difficulty: item.difficulty,
      tag_confidence: item.tag_confidence,
      question_text: item.question_text,
      bookmarked: isBookmarked(item.id) ? "yes" : "no",
    }));
    downloadCsv(
      `neet-question-bank-${stamp}.csv`,
      [
        { key: "id", label: "id" },
        { key: "subject", label: "subject" },
        { key: "topic", label: "topic" },
        { key: "question_type", label: "question_type" },
        { key: "difficulty", label: "difficulty" },
        { key: "tag_confidence", label: "tag_confidence" },
        { key: "bookmarked", label: "bookmarked" },
        { key: "question_text", label: "question_text" },
      ],
      rows
    );
  }

  function exportSessionReportCsv(mode) {
    const session = getSession(mode);
    const report = session?.report;
    if (!report || !Array.isArray(report.details) || !report.details.length) {
      return;
    }

    const stamp = new Date().toISOString().slice(0, 10);
    downloadCsv(
      `neet-${mode}-session-${stamp}.csv`,
      [
        { key: "question_id", label: "question_id" },
        { key: "subject", label: "subject" },
        { key: "topic", label: "topic" },
        { key: "difficulty", label: "difficulty" },
        { key: "selected", label: "selected" },
        { key: "answer", label: "answer" },
        { key: "correct", label: "correct" },
        { key: "time_spent_sec", label: "time_spent_sec" },
        { key: "mistake_type", label: "mistake_type" },
      ],
      report.details
    );
  }

  function updatePracticeConfig(mode, patch) {
    setPracticeConfig((prev) => ({
      ...prev,
      [mode]: {
        ...prev[mode],
        ...patch,
      },
    }));
  }

  function freezeQuestionTime(session) {
    if (!session || !Array.isArray(session.question_ids) || !session.question_ids.length) {
      return session;
    }

    const qid = session.question_ids[session.index] || session.question_ids[0];
    if (!qid) {
      return session;
    }

    const elapsed = Math.max(0, Math.floor((Date.now() - Number(session.activeStartedAt || Date.now())) / 1000));
    return {
      ...session,
      activeStartedAt: Date.now(),
      timeSpent: {
        ...(session.timeSpent || {}),
        [qid]: Number((session.timeSpent || {})[qid] || 0) + elapsed,
      },
    };
  }

  function getSession(mode) {
    return practiceSessions[mode] || null;
  }

  async function startPractice(mode) {
    const cfg = practiceConfig[mode];
    if (!cfg) {
      return;
    }

    const request = {
      user_name: userName,
      mode,
      count: Number(cfg.count || PRACTICE_CONFIG[mode].count),
      duration_minutes: Number(cfg.duration || PRACTICE_CONFIG[mode].duration),
      subjects: cfg.subject ? [cfg.subject] : [],
      topics: cfg.topic ? [cfg.topic] : [],
      question_types: cfg.questionType ? [cfg.questionType] : [],
      difficulties: cfg.difficulty ? [cfg.difficulty] : [],
      search_text: cfg.searchText || "",
      only_tagged: Boolean(cfg.onlyTagged),
      only_pyq: Boolean(PRACTICE_CONFIG[mode].onlyPyq),
    };

    setBusyAction(`start-${mode}`);
    try {
      const data = await apiPost("/api/practice/start", request);
      const session = {
        ...data,
        answers: {},
        index: 0,
        page: 0,
        submitted: false,
        report: null,
        timeSpent: {},
        flags: {},
        activeStartedAt: Date.now(),
      };

      setPracticeSessions((prev) => ({
        ...prev,
        [mode]: session,
      }));
      setError("");
    } catch (err) {
      setError(err.message || `Failed to start ${mode} session`);
    } finally {
      setBusyAction("");
    }
  }

  function setPracticeAnswer(mode, qid, value) {
    setPracticeSessions((prev) => {
      const session = prev[mode];
      if (!session || session.submitted) {
        return prev;
      }
      return {
        ...prev,
        [mode]: {
          ...session,
          answers: {
            ...session.answers,
            [qid]: Number(value || 0),
          },
        },
      };
    });
  }

  function navigatePractice(mode, delta) {
    setPracticeSessions((prev) => {
      const session = prev[mode];
      if (!session || session.submitted) {
        return prev;
      }

      const frozen = freezeQuestionTime(session);
      const nextIndex = clamp(
        Number(frozen.index || 0) + Number(delta || 0),
        0,
        Math.max(0, Number((frozen.question_ids || []).length || 1) - 1)
      );

      return {
        ...prev,
        [mode]: {
          ...frozen,
          index: nextIndex,
          activeStartedAt: Date.now(),
        },
      };
    });
  }

  function togglePracticeFlag(mode, qid) {
    const numericId = Number(qid || 0);
    if (!numericId) {
      return;
    }

    setPracticeSessions((prev) => {
      const session = prev[mode];
      if (!session || session.submitted) {
        return prev;
      }

      const current = Boolean(session.flags?.[numericId]);
      return {
        ...prev,
        [mode]: {
          ...session,
          flags: {
            ...(session.flags || {}),
            [numericId]: !current,
          },
        },
      };
    });
  }

  function jumpToNextFlagged(mode) {
    setPracticeSessions((prev) => {
      const session = prev[mode];
      if (!session || session.submitted) {
        return prev;
      }

      const questionIds = Array.isArray(session.question_ids) ? session.question_ids : [];
      const flaggedIds = questionIds.filter((qid) => Boolean(session.flags?.[qid]));
      if (!flaggedIds.length) {
        return prev;
      }

      const currentIndex = Number(session.index || 0);
      const currentQid = questionIds[currentIndex];
      const startAt = Math.max(0, flaggedIds.indexOf(currentQid));
      const nextFlagged = flaggedIds[(startAt + 1) % flaggedIds.length];
      const targetIndex = Math.max(0, questionIds.indexOf(nextFlagged));

      return {
        ...prev,
        [mode]: {
          ...session,
          index: targetIndex,
          activeStartedAt: Date.now(),
        },
      };
    });
  }

  function setOmrPage(mode, delta) {
    setPracticeSessions((prev) => {
      const session = prev[mode];
      if (!session || session.submitted) {
        return prev;
      }
      const pageSize = 20;
      const totalPages = Math.max(1, Math.ceil((session.question_ids || []).length / pageSize));
      const currentPage = Number(session.page || 0);
      const nextPage = clamp(currentPage + Number(delta || 0), 0, totalPages - 1);
      return {
        ...prev,
        [mode]: {
          ...session,
          page: nextPage,
        },
      };
    });
  }

  async function submitPractice(mode) {
    const existing = getSession(mode);
    if (!existing || existing.submitted) {
      return;
    }

    const session = freezeQuestionTime(existing);
    setPracticeSessions((prev) => ({
      ...prev,
      [mode]: session,
    }));

    const payload = {
      user_name: userName,
      mode,
      session_id: session.session_id,
      question_ids: session.question_ids,
      answers: session.answers,
      time_spent_sec: session.timeSpent,
    };

    setBusyAction(`submit-${mode}`);
    try {
      const report = await apiPost("/api/practice/submit", payload);
      setPracticeSessions((prev) => ({
        ...prev,
        [mode]: {
          ...session,
          submitted: true,
          report,
        },
      }));

      await Promise.all([
        refreshOverview(),
        refreshWeakness(),
        refreshTimeAnalytics(),
        refreshForecast(),
        refreshCoaching(),
        refreshDailyStreak(),
        refreshDailySharePayload(),
      ]);
      setError("");
    } catch (err) {
      setError(err.message || `Failed to submit ${mode} session`);
    } finally {
      setBusyAction("");
    }
  }

  function clearPractice(mode) {
    setPracticeSessions((prev) => ({
      ...prev,
      [mode]: null,
    }));
  }

  async function saveBankAttempt() {
    if (!selectedBankQid || !bankQuestion) {
      return;
    }

    const answerValue = Number(selectedBankOption || 0);
    const selected = answerValue > 0 ? answerValue : null;

    setBusyAction("bank-attempt");
    try {
      await apiPost("/api/attempts/log", {
        user_name: userName,
        mode: "bank-practice",
        question_id: Number(selectedBankQid),
        selected_option: selected,
        is_correct: selectedBankManualCorrect ? true : null,
        time_spent_sec: 0,
      });

      setSelectedBankManualCorrect(false);
      await Promise.all([refreshWeakness(), refreshTimeAnalytics(), refreshForecast(), refreshCoaching()]);
      setError("");
    } catch (err) {
      setError(err.message || "Failed to save practice attempt");
    } finally {
      setBusyAction("");
    }
  }

  async function askAiTutor() {
    const prompt = String(aiPrompt || "").trim();
    if (!prompt) {
      return;
    }

    setBusyAction("ai-ask");
    try {
      const data = await apiPost("/api/ai/ask", {
        user_name: userName,
        prompt,
        context: `Current tagged ${overview?.tagged_questions || 0}/${overview?.total_questions || 0}, pending ${overview?.pending_questions || 0}`,
      });
      setAiReply(String(data.reply || ""));
      setError("");
    } catch (err) {
      setError(err.message || "AI tutor request failed");
    } finally {
      setBusyAction("");
    }
  }

  async function explainWithAi() {
    const qid = Number(aiExplainQid || 0);
    if (!qid) {
      return;
    }

    setBusyAction("ai-explain");
    try {
      const data = await apiPost("/api/ai/explain", {
        user_name: userName,
        question_id: qid,
        selected_option: Number(aiExplainOption || 0) || null,
      });
      setAiExplainReply(String(data.explanation || ""));
      setError("");
    } catch (err) {
      setError(err.message || "AI explanation failed");
    } finally {
      setBusyAction("");
    }
  }

  useEffect(() => {
    refreshOverview();
    refreshMeta();
    const timer = window.setInterval(() => {
      refreshOverview();
    }, 20000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(BOOKMARKS_KEY);
      if (!raw) {
        return;
      }
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) {
        return;
      }
      const safe = parsed.map((item) => Number(item)).filter((item) => Number.isFinite(item) && item > 0);
      setBookmarks(Array.from(new Set(safe)).sort((a, b) => a - b));
    } catch {
      setBookmarks([]);
    }
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(BOOKMARKS_KEY, JSON.stringify(bookmarks || []));
    } catch {
      // Ignore storage errors to keep UI responsive.
    }
  }, [bookmarks]);

  useEffect(() => {
    const timer = window.setInterval(() => setClockTick((value) => value + 1), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    refreshTopics(subject);
    setTopic("");
    setPage(0);
  }, [subject]);

  useEffect(() => {
    setPage(0);
  }, [search, questionType, difficulty, onlyTagged]);

  useEffect(() => {
    refreshQuestions();
  }, [search, subject, topic, questionType, difficulty, onlyTagged, page]);

  useEffect(() => {
    refreshMeta();
    refreshOverview();
  }, [userName]);

  useEffect(() => {
    if (activeTab === "data") {
      refreshDataLab();
    } else if (activeTab === "verification") {
      refreshVerificationSnapshot();
    } else if (activeTab === "time") {
      refreshTimeAnalytics();
    } else if (activeTab === "weak") {
      refreshWeakness();
    } else if (activeTab === "revision") {
      refreshRevisionPlan();
      refreshWeakness();
    } else if (activeTab === "flash") {
      refreshFlashcards();
    } else if (activeTab === "daily") {
      refreshDailyStreak();
      refreshDailySharePayload();
    } else if (activeTab === "forecast") {
      refreshForecast();
    } else if (activeTab === "coaching") {
      refreshCoaching();
    }
  }, [activeTab, userName]);

  const totalPages = Math.max(1, Math.ceil(totalQuestions / PAGE_SIZE));

  function renderDashboardTab() {
    return (
      <>
        <section className="metrics-grid">
          <MetricCard
            label="Total Questions"
            value={overview?.total_questions ?? "-"}
            hint="Questions available in your SQLite bank"
          />
          <MetricCard
            label="Tagged"
            value={overview?.tagged_questions ?? "-"}
            hint="Rows with confidence greater than zero"
          />
          <MetricCard
            label="Pending"
            value={overview?.pending_questions ?? "-"}
            hint="Rows still waiting for metadata tags"
          />
          <MetricCard
            label="Average Confidence"
            value={overview ? Number(overview.average_tag_confidence || 0).toFixed(3) : "-"}
            hint="Mean confidence among tagged questions"
          />
        </section>

        <section className="panel-grid">
          <article className="panel card-enter">
            <div className="panel-head">
              <h2>Tagging Progress</h2>
              <span className="progress-pct">{taggingProgress?.progress_pct ?? 0}%</span>
            </div>
            <div className="progress-track">
              <div className="progress-fill" style={{ width: `${taggingProgress?.progress_pct ?? 0}%` }} />
            </div>
            <div className="progress-meta">
              <span>Tagged: {taggingProgress?.tagged ?? 0}</span>
              <span>Pending: {taggingProgress?.pending ?? 0}</span>
            </div>
            <div className="band-list">
              {(taggingProgress?.confidence_bands || []).map((band) => (
                <div key={band.band} className="band-item">
                  <span>{band.band}</span>
                  <strong>{band.total}</strong>
                </div>
              ))}
            </div>
          </article>

          <article className="panel card-enter">
            <h2>Subject Distribution</h2>
            <div className="subject-list">
              {(overview?.by_subject || []).map((item) => (
                <SubjectBar key={item.subject} item={item} max={maxSubjectCount} />
              ))}
            </div>
          </article>
        </section>
      </>
    );
  }

  function renderDataLabTab() {
    const files = dataSummary?.files || {};
    return (
      <section className="panel card-enter">
        <h2>Question Data + Answer Key Management</h2>
        <p className="muted">
          This web stack reads your same SQLite source used by the tagging and analytics pipeline.
        </p>

        <div className="metrics-grid compact-grid">
          <MetricCard label="DB Questions" value={dataSummary?.total_questions ?? "-"} hint="Rows in questions table" />
          <MetricCard
            label="Answer Key Coverage"
            value={dataSummary?.answer_key_coverage ?? "-"}
            hint="Distinct question IDs in answer_keys"
          />
          <MetricCard label="Attempt Logs" value={dataSummary?.attempt_logs ?? "-"} hint="Rows in ui_attempts" />
          <MetricCard label="Session Reports" value={dataSummary?.session_reports ?? "-"} hint="Rows in ui_sessions" />
        </div>

        <DataTable
          columns={[
            { key: "name", label: "Artifact" },
            { key: "present", label: "Present" },
          ]}
          rows={[
            { key: "questions", name: "questions.json", present: String(files.questions_json) },
            { key: "state", name: "study_state.json", present: String(files.study_state_json) },
            { key: "manifest", name: "manifest.json", present: String(files.manifest_json) },
          ]}
          emptyText="No file metadata available"
        />
      </section>
    );
  }

  function renderVerificationTab() {
    const checks = verificationReport?.checks || [];
    return (
      <section className="panel card-enter">
        <div className="panel-head">
          <h2>Project Verification Layer</h2>
          <div className="stack-inline">
            <button className="nav-btn" onClick={() => runVerification({ deep: false, remote: false })} disabled={busyAction === "verification"}>
              Run Quick
            </button>
            <button className="nav-btn" onClick={() => runVerification({ deep: true, remote: true })} disabled={busyAction === "verification"}>
              Run Full
            </button>
          </div>
        </div>

        <div className="metrics-grid compact-grid">
          <MetricCard label="Status" value={String(verificationReport?.status || "-").toUpperCase()} hint="Overall verification state" />
          <MetricCard label="Passed" value={verificationReport?.passed ?? "-"} hint="Checks passed" />
          <MetricCard label="Warnings" value={verificationReport?.warnings ?? "-"} hint="Checks with warnings" />
          <MetricCard label="Failed" value={verificationReport?.failed ?? "-"} hint="Checks failed" />
        </div>

        <DataTable
          columns={[
            { key: "name", label: "Check" },
            { key: "status", label: "Status" },
            { key: "message", label: "Message" },
          ]}
          rows={checks}
          emptyText="No verification report yet"
        />
      </section>
    );
  }

  function renderPracticeTab(mode) {
    const cfg = practiceConfig[mode];
    const session = getSession(mode);
    const modeSpec = PRACTICE_CONFIG[mode];

    if (!cfg || !modeSpec) {
      return <p className="muted">Mode not available</p>;
    }

    const questionCount = session?.questions?.length || 0;
    const startMs = session?.started_at ? new Date(session.started_at).getTime() : 0;
    const elapsedSec = startMs ? Math.max(0, Math.floor((Date.now() - startMs) / 1000)) : 0;
    const totalSec = Number(session?.duration_minutes || 0) * 60;
    const remaining = Math.max(0, totalSec - elapsedSec);

    const currentQuestion = session?.questions?.[Number(session?.index || 0)] || null;
    const flaggedIds = (session?.question_ids || []).filter((qid) => Boolean(session?.flags?.[qid]));
    const flaggedCount = flaggedIds.length;

    return (
      <section className="panel card-enter">
        <h2>{modeSpec.title}</h2>

        {!session || session.submitted ? (
          <>
            <div className="filters two-row">
              <select className="field" value={cfg.subject} onChange={(event) => updatePracticeConfig(mode, { subject: event.target.value })}>
                <option value="">All subjects</option>
                {(meta.subjects || []).map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>

              <select className="field" value={cfg.topic} onChange={(event) => updatePracticeConfig(mode, { topic: event.target.value })}>
                <option value="">All topics</option>
                {(meta.topics || []).slice(0, 300).map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>

              <select
                className="field"
                value={cfg.questionType}
                onChange={(event) => updatePracticeConfig(mode, { questionType: event.target.value })}
              >
                <option value="">All question types</option>
                {questionTypeOptions.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>

              <select
                className="field"
                value={cfg.difficulty}
                onChange={(event) => updatePracticeConfig(mode, { difficulty: event.target.value })}
              >
                <option value="">All difficulties</option>
                {(meta.difficulties || []).map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>

              <input
                className="field"
                type="number"
                min={1}
                max={300}
                value={cfg.count}
                onChange={(event) => updatePracticeConfig(mode, { count: clamp(Number(event.target.value || 1), 1, 300) })}
                placeholder="Question count"
              />

              <input
                className="field"
                type="number"
                min={5}
                max={300}
                value={cfg.duration}
                onChange={(event) => updatePracticeConfig(mode, { duration: clamp(Number(event.target.value || 5), 5, 300) })}
                placeholder="Duration in minutes"
              />

              <input
                className="field"
                type="text"
                value={cfg.searchText}
                onChange={(event) => updatePracticeConfig(mode, { searchText: event.target.value })}
                placeholder="Search text"
              />

              <label className="checkbox-wrap">
                <input
                  type="checkbox"
                  checked={Boolean(cfg.onlyTagged)}
                  onChange={(event) => updatePracticeConfig(mode, { onlyTagged: event.target.checked })}
                />
                <span>Only tagged</span>
              </label>
            </div>

            <div className="stack-inline mt-12">
              <button className="refresh-btn" onClick={() => startPractice(mode)} disabled={busyAction === `start-${mode}`}>
                {busyAction === `start-${mode}` ? "Starting..." : `Start ${modeSpec.title}`}
              </button>
              {session?.submitted ? (
                <button className="nav-btn" onClick={() => clearPractice(mode)}>
                  Clear Previous Session
                </button>
              ) : null}
            </div>

            {session?.submitted && session.report ? renderSessionReport(session.report, mode) : null}
          </>
        ) : (
          <>
            <div className="stack-inline">
              <TagChip text={`Mode: ${session.mode}`} tone="accent" />
              <TagChip text={`Session: ${session.session_id}`} tone="default" />
              <TagChip text={`Questions: ${questionCount}`} tone="default" />
              <TagChip text={`Time left: ${formatSeconds(remaining)}`} tone="soft" />
              <TagChip text={`Flagged: ${flaggedCount}`} tone="warm" />
            </div>

            {mode === "omr" ? (
              <div className="omr-panel">
                {(() => {
                  const pageSize = 20;
                  const page = Number(session.page || 0);
                  const totalPages = Math.max(1, Math.ceil(questionCount / pageSize));
                  const start = page * pageSize;
                  const end = Math.min(questionCount, start + pageSize);
                  const pageQuestions = session.questions.slice(start, end);
                  return (
                    <>
                      <p className="muted">OMR page {page + 1} / {totalPages}</p>
                      {pageQuestions.map((question) => (
                        <div key={question.id} className="omr-row">
                          <div>
                            <strong>Q{question.id}</strong> <span className="muted">[{question.subject}]</span>
                          </div>
                          <select
                            className="field"
                            value={Number(session.answers?.[question.id] || 0)}
                            onChange={(event) => setPracticeAnswer(mode, question.id, Number(event.target.value || 0))}
                          >
                            <option value={0}>Unmarked</option>
                            <option value={1}>1</option>
                            <option value={2}>2</option>
                            <option value={3}>3</option>
                            <option value={4}>4</option>
                          </select>
                        </div>
                      ))}

                      <div className="stack-inline mt-12">
                        <button className="nav-btn" onClick={() => setOmrPage(mode, -1)} disabled={page <= 0}>
                          Previous Page
                        </button>
                        <button className="nav-btn" onClick={() => setOmrPage(mode, 1)} disabled={page + 1 >= totalPages}>
                          Next Page
                        </button>
                        <button className="refresh-btn" onClick={() => submitPractice(mode)} disabled={busyAction === `submit-${mode}`}>
                          {busyAction === `submit-${mode}` ? "Submitting..." : "Submit OMR Session"}
                        </button>
                      </div>
                    </>
                  );
                })()}
              </div>
            ) : (
              <>
                {currentQuestion ? (
                  <article className="question-card mt-12">
                    <div className="question-meta">
                      <TagChip text={`Q ${Number(session.index || 0) + 1}/${questionCount}`} tone="default" />
                      <TagChip text={currentQuestion.subject || "Unknown"} tone="accent" />
                      <TagChip text={currentQuestion.topic || "unknown"} tone="default" />
                      <TagChip text={`Type: ${currentQuestion.question_type || "unknown"}`} tone="soft" />
                      <DifficultyBadge difficulty={currentQuestion.difficulty} />
                      {session.flags?.[currentQuestion.id] ? <TagChip text="Flagged" tone="alert" /> : null}
                    </div>

                    <p className="question-text">{currentQuestion.question_text}</p>

                    <div className="radio-grid">
                      {[0, 1, 2, 3, 4].map((value) => {
                        const selectedValue = Number(session.answers?.[currentQuestion.id] || 0);
                        const label = value === 0 ? "Not answered" : `${value}. ${currentQuestion.options?.[value - 1] || ""}`;
                        return (
                          <label key={value} className="radio-option">
                            <input
                              type="radio"
                              name={`practice-${mode}-${currentQuestion.id}`}
                              checked={selectedValue === value}
                              onChange={() => setPracticeAnswer(mode, currentQuestion.id, value)}
                            />
                            <span>{label}</span>
                          </label>
                        );
                      })}
                    </div>
                  </article>
                ) : (
                  <p className="muted">No active question.</p>
                )}

                <div className="stack-inline mt-12">
                  <button className="nav-btn" onClick={() => togglePracticeFlag(mode, currentQuestion?.id)}>
                    {session.flags?.[currentQuestion?.id] ? "Remove Flag" : "Flag for Review"}
                  </button>
                  <button className="nav-btn" onClick={() => jumpToNextFlagged(mode)} disabled={!flaggedCount}>
                    Next Flagged
                  </button>
                  <button
                    className="nav-btn"
                    onClick={() => navigatePractice(mode, -1)}
                    disabled={Number(session.index || 0) <= 0}
                  >
                    Previous
                  </button>
                  <button
                    className="nav-btn"
                    onClick={() => navigatePractice(mode, 1)}
                    disabled={Number(session.index || 0) + 1 >= questionCount}
                  >
                    Next
                  </button>
                  <button className="refresh-btn" onClick={() => submitPractice(mode)} disabled={busyAction === `submit-${mode}`}>
                    {busyAction === `submit-${mode}` ? "Submitting..." : "Submit Session"}
                  </button>
                </div>
              </>
            )}
          </>
        )}
      </section>
    );
  }

  function renderSessionReport(report, mode) {
    if (!report) {
      return null;
    }

    return (
      <div className="report-block mt-12">
        <div className="metrics-grid compact-grid">
          <MetricCard label="Attempted" value={report.attempted} hint="Answered questions" />
          <MetricCard label="Graded" value={report.graded} hint="Questions with answer key" />
          <MetricCard label="Correct" value={report.correct} hint="Correct graded answers" />
          <MetricCard label="Score" value={report.score} hint={`Accuracy ${report.accuracy || 0}%`} />
        </div>

        <DataTable
          columns={[
            { key: "question_id", label: "QID" },
            { key: "subject", label: "Subject" },
            { key: "topic", label: "Topic" },
            { key: "selected", label: "Selected" },
            { key: "answer", label: "Answer" },
            {
              key: "correct",
              label: "Result",
              render: (value) => {
                if (value === true) return "Correct";
                if (value === false) return "Wrong";
                return "Ungraded";
              },
            },
            { key: "mistake_type", label: "Mistake Type" },
          ]}
          rows={report.details || []}
          emptyText="No report details"
        />

        <div className="stack-inline mt-12">
          <button className="nav-btn" onClick={() => exportSessionReportCsv(mode)}>
            Export Session CSV
          </button>
        </div>
      </div>
    );
  }

  function renderQuestionBankTab() {
    return (
      <section className="explorer card-enter">
        <div className="explorer-head">
          <h2>Searchable Question Bank + Quick Practice</h2>
          <p>Filter by subject, chapter, difficulty, and search keyword.</p>
        </div>

        <div className="filters">
          <input
            className="field"
            type="text"
            placeholder="Search question text"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />

          <select className="field" value={subject} onChange={(event) => setSubject(event.target.value)}>
            <option value="">All subjects</option>
            {subjectOptions.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>

          <select className="field" value={topic} onChange={(event) => setTopic(event.target.value)}>
            <option value="">All topics</option>
            {topicOptions.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>

          <select className="field" value={questionType} onChange={(event) => setQuestionType(event.target.value)}>
            <option value="">All question types</option>
            {questionTypeOptions.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>

          <select className="field" value={difficulty} onChange={(event) => setDifficulty(event.target.value)}>
            <option value="">All difficulties</option>
            <option value="easy">easy</option>
            <option value="medium">medium</option>
            <option value="hard">hard</option>
            <option value="unknown">unknown</option>
          </select>

          <label className="checkbox-wrap">
            <input type="checkbox" checked={onlyTagged} onChange={(event) => setOnlyTagged(event.target.checked)} />
            <span>Only tagged questions</span>
          </label>

          <label className="checkbox-wrap">
            <input type="checkbox" checked={onlyBookmarked} onChange={(event) => setOnlyBookmarked(event.target.checked)} />
            <span>Only bookmarked on current page</span>
          </label>
        </div>

        <div className="results-head">
          <strong>{onlyBookmarked ? visibleQuestions.length : totalQuestions}</strong>
          <span>matching questions</span>
        </div>

        <div className="stack-inline mt-12">
          <button className="nav-btn" onClick={exportCurrentQuestionsCsv}>
            Export Current List CSV
          </button>
          <TagChip text={`Bookmarks: ${bookmarks.length}`} tone="warm" />
        </div>

        <div className="question-list">
          {visibleQuestions.length === 0 ? (
            <p className="muted">No questions found for this filter set.</p>
          ) : (
            visibleQuestions.map((question) => (
              <article
                key={question.id}
                className={`question-card ${selectedBankQid === question.id ? "selected-question" : ""}`}
                onClick={() => setSelectedBankQid(question.id)}
                role="button"
                tabIndex={0}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    setSelectedBankQid(question.id);
                  }
                }}
              >
                <div className="question-meta">
                  <TagChip text={`#${question.id}`} tone="default" />
                  <TagChip text={question.subject || "Unknown"} tone="accent" />
                  <TagChip text={question.topic || "unknown topic"} tone="default" />
                  <TagChip text={`Type: ${question.question_type || "unknown"}`} tone="soft" />
                  <DifficultyBadge difficulty={question.difficulty} />
                  <TagChip text={`Bloom: ${question.bloom_level || "unknown"}`} tone="soft" />
                  <TagChip text={`Conf: ${(Number(question.tag_confidence || 0)).toFixed(2)}`} tone="soft" />
                </div>
                <p className="question-text">{question.question_text}</p>
                {Array.isArray(question.options) && question.options.length ? (
                  <ol className="options-list">
                    {question.options.map((option, idx) => (
                      <li key={`${question.id}-${idx}`}>{option}</li>
                    ))}
                  </ol>
                ) : null}
                <div className="stack-inline mt-12">
                  <button
                    className="bookmark-btn"
                    onClick={(event) => {
                      event.stopPropagation();
                      toggleBookmark(question.id);
                    }}
                  >
                    {isBookmarked(question.id) ? "Unbookmark" : "Bookmark"}
                  </button>
                </div>
              </article>
            ))
          )}
        </div>

        <footer className="pagination">
          <button className="nav-btn" onClick={() => setPage((p) => Math.max(0, p - 1))} disabled={page === 0}>
            Previous
          </button>
          <span>
            Page {page + 1} / {totalPages}
          </span>
          <button
            className="nav-btn"
            onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
            disabled={page + 1 >= totalPages}
          >
            Next
          </button>
        </footer>

        {bankQuestion ? (
          <div className="bank-practice mt-16">
            <h3>Quick Practice Save</h3>
            <p className="muted">Selected question: {bankQuestion.id}</p>
            <select
              className="field"
              value={selectedBankOption}
              onChange={(event) => setSelectedBankOption(Number(event.target.value || 0))}
            >
              <option value={0}>Not answered</option>
              <option value={1}>Option 1</option>
              <option value={2}>Option 2</option>
              <option value={3}>Option 3</option>
              <option value={4}>Option 4</option>
            </select>
            <label className="checkbox-wrap mt-8">
              <input
                type="checkbox"
                checked={selectedBankManualCorrect}
                onChange={(event) => setSelectedBankManualCorrect(event.target.checked)}
              />
              <span>Mark as correct manually when no answer key is available</span>
            </label>
            <button className="refresh-btn mt-12" onClick={saveBankAttempt} disabled={busyAction === "bank-attempt"}>
              {busyAction === "bank-attempt" ? "Saving..." : "Save Practice Attempt"}
            </button>
          </div>
        ) : null}
      </section>
    );
  }

  function renderTopicTab() {
    return (
      <section className="panel card-enter">
        <h2>Topic Coverage Analyzer</h2>
        <DataTable
          columns={[
            { key: "topic", label: "Topic" },
            { key: "total", label: "Questions" },
            {
              key: "tagged",
              label: "Tagged",
              render: (value, row) => `${value} (${toPct(value, row.total)}%)`,
            },
          ]}
          rows={topicsData}
          emptyText="No topic coverage data"
        />
      </section>
    );
  }

  function renderTimeTab() {
    return (
      <section className="panel card-enter">
        <h2>Time Analytics Per Question</h2>
        <p className="muted">Average time: {Number(timeAnalytics?.average_time_sec || 0).toFixed(2)} sec</p>

        <h3 className="subheading">Avg Time by Subject</h3>
        <DataTable
          columns={[
            { key: "subject", label: "Subject" },
            { key: "avg_time_sec", label: "Avg Time (sec)" },
            { key: "attempts", label: "Attempts" },
          ]}
          rows={timeAnalytics?.by_subject || []}
          emptyText="No subject analytics yet"
        />

        <h3 className="subheading">Slowest Topics</h3>
        <DataTable
          columns={[
            { key: "topic", label: "Topic" },
            { key: "avg_time_sec", label: "Avg Time (sec)" },
            { key: "attempts", label: "Attempts" },
          ]}
          rows={timeAnalytics?.by_topic || []}
          emptyText="No topic analytics yet"
        />
      </section>
    );
  }

  function renderWeaknessTab() {
    return (
      <section className="panel card-enter">
        <h2>Weakness Tracker + 7-Day Recovery Plan</h2>
        <h3 className="subheading">Weak Topics</h3>
        <DataTable
          columns={[
            { key: "topic", label: "Topic" },
            { key: "attempts", label: "Attempts" },
            { key: "wrong", label: "Wrong" },
            { key: "accuracy", label: "Accuracy (%)" },
          ]}
          rows={weakness?.items || []}
          emptyText="No graded attempts yet"
        />

        <h3 className="subheading">Auto-Generated 7-Day Plan</h3>
        <DataTable
          columns={[
            { key: "day", label: "Day" },
            { key: "focus_topic", label: "Focus Topic" },
            { key: "tasks", label: "Tasks" },
            { key: "goal", label: "Goal" },
          ]}
          rows={weakness?.recovery_plan_7d || []}
          emptyText="No recovery plan yet"
        />
      </section>
    );
  }

  function renderRevisionTab() {
    return (
      <section className="panel card-enter">
        <h2>Revision Scheduler with Calendar</h2>

        <div className="filters two-row">
          <input
            className="field"
            type="number"
            min={1}
            max={365}
            value={revisionDays}
            onChange={(event) => setRevisionDays(clamp(Number(event.target.value || 1), 1, 365))}
            placeholder="Plan days"
          />
          <input
            className="field"
            type="number"
            min={10}
            max={200}
            value={revisionTarget}
            onChange={(event) => setRevisionTarget(clamp(Number(event.target.value || 10), 10, 200))}
            placeholder="Daily MCQ target"
          />
          <button className="refresh-btn" onClick={generateRevisionPlan} disabled={busyAction === "revision-generate"}>
            {busyAction === "revision-generate" ? "Generating..." : "Generate Revision Plan"}
          </button>
        </div>

        <p className="muted">Plan completion: {Number(revisionPlan?.completion_pct || 0).toFixed(2)}%</p>

        {(revisionPlan?.items || []).length ? (
          <div className="revision-list">
            {revisionPlan.items.map((item) => (
              <article key={item.date} className="revision-card">
                <div>
                  <strong>{item.date}</strong>
                  <p>{item.topic}</p>
                  <ul>
                    {(item.tasks || []).map((task, idx) => (
                      <li key={`${item.date}-${idx}`}>{task}</li>
                    ))}
                  </ul>
                </div>
                <button
                  className="nav-btn"
                  onClick={() => markRevisionDay(item.date, !item.completed)}
                  disabled={busyAction === `revision-${item.date}`}
                >
                  {item.completed ? "Mark Incomplete" : "Mark Completed"}
                </button>
              </article>
            ))}
          </div>
        ) : (
          <p className="muted">No revision plan yet.</p>
        )}
      </section>
    );
  }

  function renderFlashcardsTab() {
    const dueCards = (flashcards?.items || []).filter((item) => {
      const dueDate = new Date(`${item.next_due}T00:00:00`);
      const now = new Date();
      const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      return dueDate <= today;
    });

    const firstDue = dueCards[0];

    return (
      <section className="panel card-enter">
        <h2>Smart Flashcards</h2>
        <div className="stack-inline">
          <MetricCard label="Total" value={flashcards?.total ?? 0} hint="All flashcards" />
          <MetricCard label="Due Today" value={flashcards?.due_today ?? 0} hint="Cards scheduled for review" />
        </div>

        <button className="refresh-btn mt-12" onClick={generateFlashcards} disabled={busyAction === "flash-generate"}>
          {busyAction === "flash-generate" ? "Generating..." : "Generate from Wrong Attempts"}
        </button>

        {firstDue ? (
          <article className="question-card mt-16">
            <div className="question-meta">
              <TagChip text={`Q ${firstDue.question_id}`} tone="default" />
              <TagChip text={firstDue.subject} tone="accent" />
              <TagChip text={firstDue.topic} tone="default" />
              <TagChip text={`Next due ${firstDue.next_due}`} tone="soft" />
            </div>
            <p className="question-text">{firstDue.question_text}</p>
            <p className="muted">Answer key: {firstDue.answer_key ?? "Not available"}</p>
            <div className="stack-inline mt-12">
              <button className="nav-btn" onClick={() => reviewFlashcard(firstDue.question_id, "again")}>Again</button>
              <button className="nav-btn" onClick={() => reviewFlashcard(firstDue.question_id, "good")}>Good</button>
              <button className="nav-btn" onClick={() => reviewFlashcard(firstDue.question_id, "easy")}>Easy</button>
            </div>
          </article>
        ) : (
          <p className="muted mt-12">No due flashcards right now.</p>
        )}
      </section>
    );
  }

  function renderDailyTab() {
    const shareText = String(dailySharePayload?.message || "").trim();
    const whatsappUrl = shareText ? `https://wa.me/?text=${encodeURIComponent(shareText)}` : "";
    const telegramUrl = shareText
      ? `https://t.me/share/url?url=${encodeURIComponent("https://localhost:5173")}&text=${encodeURIComponent(shareText)}`
      : "";

    return (
      <>
        <section className="panel card-enter">
          <h2>Daily Quiz Bot</h2>
          <p className="muted">Current daily streak: {dailyStreak}</p>

          <div className="ai-block mt-12">
            <h3 className="subheading">Telegram / WhatsApp Share Kit</h3>
            <p className="muted">Use this auto-generated message to push daily updates to your student group.</p>
            <pre className="ai-reply">{shareText || "No share payload yet. Submit one daily quiz first."}</pre>
            <div className="stack-inline mt-12">
              <button className="nav-btn" onClick={copyDailyShareMessage} disabled={!shareText}>
                Copy Message
              </button>
              <a className="nav-btn link-btn" href={whatsappUrl || "#"} target="_blank" rel="noreferrer">
                Share to WhatsApp
              </a>
              <a className="nav-btn link-btn" href={telegramUrl || "#"} target="_blank" rel="noreferrer">
                Share to Telegram
              </a>
            </div>
          </div>
        </section>
        {renderPracticeTab("daily-quiz")}
      </>
    );
  }

  function renderAiTab() {
    return (
      <section className="panel card-enter">
        <h2>AI Tutor (Groq)</h2>

        <div className="ai-block">
          <h3 className="subheading">Ask AI Tutor</h3>
          <textarea
            className="field multiline"
            value={aiPrompt}
            onChange={(event) => setAiPrompt(event.target.value)}
            placeholder="Ask any NEET doubt, strategy, or revision question"
          />
          <button className="refresh-btn mt-12" onClick={askAiTutor} disabled={busyAction === "ai-ask"}>
            {busyAction === "ai-ask" ? "Asking..." : "Ask Groq Tutor"}
          </button>
          {aiReply ? <pre className="ai-reply">{aiReply}</pre> : null}
        </div>

        <div className="ai-block mt-16">
          <h3 className="subheading">Explain a Question</h3>
          <select className="field" value={aiExplainQid} onChange={(event) => setAiExplainQid(event.target.value)}>
            <option value="">Select question ID from current bank page</option>
            {questions.map((item) => (
              <option key={item.id} value={item.id}>
                {item.id} | {item.subject} | {item.topic}
              </option>
            ))}
          </select>

          <select
            className="field mt-8"
            value={aiExplainOption}
            onChange={(event) => setAiExplainOption(Number(event.target.value || 0))}
          >
            <option value={0}>Skip selected option</option>
            <option value={1}>Selected option 1</option>
            <option value={2}>Selected option 2</option>
            <option value={3}>Selected option 3</option>
            <option value={4}>Selected option 4</option>
          </select>

          <button className="refresh-btn mt-12" onClick={explainWithAi} disabled={busyAction === "ai-explain"}>
            {busyAction === "ai-explain" ? "Explaining..." : "Explain with AI"}
          </button>
          {aiExplainReply ? <pre className="ai-reply">{aiExplainReply}</pre> : null}
        </div>
      </section>
    );
  }

  function renderForecastTab() {
    return (
      <section className="panel card-enter">
        <h2>Performance Forecast</h2>

        <div className="metrics-grid compact-grid">
          <MetricCard label="Predicted Score" value={forecast?.predicted_score ?? 0} hint="Expected score" />
          <MetricCard
            label="Expected Range"
            value={`${forecast?.low ?? 0} - ${forecast?.high ?? 0}`}
            hint="Low to high likely band"
          />
          <MetricCard label="Confidence" value={forecast?.confidence ?? "Low"} hint="Forecast confidence" />
          <MetricCard
            label="Current Accuracy"
            value={`${Number(forecast?.current_accuracy || 0).toFixed(2)}%`}
            hint="Based on graded attempts"
          />
        </div>

        <DataTable
          columns={[
            { key: "submitted_at", label: "Submitted" },
            { key: "mode", label: "Mode" },
            { key: "score", label: "Score" },
            { key: "accuracy", label: "Accuracy (%)" },
          ]}
          rows={forecast?.history || []}
          emptyText="No forecast history yet"
        />
      </section>
    );
  }

  function renderCoachingTab() {
    return (
      <section className="panel card-enter">
        <h2>Coaching Dashboard</h2>

        <DataTable
          columns={[
            { key: "profile", label: "Profile" },
            { key: "attempts", label: "Attempts" },
            { key: "graded", label: "Graded" },
            { key: "accuracy", label: "Accuracy (%)" },
            { key: "exams", label: "Exams" },
            { key: "last_7_days_activity", label: "Last 7 Days Activity" },
          ]}
          rows={coaching?.items || []}
          emptyText="No coaching data yet"
        />

        <h3 className="subheading">Recommendations</h3>
        <ul>
          {(coaching?.recommendations || []).map((item, idx) => (
            <li key={`coach-tip-${idx}`}>{item}</li>
          ))}
        </ul>
      </section>
    );
  }

  function renderCurrentTab() {
    if (activeTab === "dashboard") return renderDashboardTab();
    if (activeTab === "data") return renderDataLabTab();
    if (activeTab === "verification") return renderVerificationTab();
    if (activeTab === "exam") return renderPracticeTab("exam");
    if (activeTab === "adaptive") return renderPracticeTab("adaptive");
    if (activeTab === "omr") return renderPracticeTab("omr");
    if (activeTab === "pyq") return renderPracticeTab("pyq");
    if (activeTab === "bank") return renderQuestionBankTab();
    if (activeTab === "topic") return renderTopicTab();
    if (activeTab === "time") return renderTimeTab();
    if (activeTab === "weak") return renderWeaknessTab();
    if (activeTab === "revision") return renderRevisionTab();
    if (activeTab === "flash") return renderFlashcardsTab();
    if (activeTab === "daily") return renderDailyTab();
    if (activeTab === "ai") return renderAiTab();
    if (activeTab === "forecast") return renderForecastTab();
    if (activeTab === "coaching") return renderCoachingTab();
    return <p className="muted">Tab not found.</p>;
  }

  return (
    <div className="page-shell">
      <div className="backdrop-orb orb-a" />
      <div className="backdrop-orb orb-b" />
      <div className="backdrop-orb orb-c" />

      <main className="layout">
        <header className="hero card-enter">
          <div>
            <p className="eyebrow">NEET Learning Platform</p>
            <h1>Full Dashboard Parity on Vite + FastAPI</h1>
            <p className="hero-text">
              Matching the Streamlit experience with a cleaner web stack: all major study tabs, real-time tagging status,
              practice modes, analytics, AI tutor, and coaching insights.
            </p>
          </div>

          <div className="hero-actions">
            <select className="field user-select" value={userName} onChange={(event) => setUserName(event.target.value)}>
              {users.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
            <button className="refresh-btn" onClick={refreshOverview}>
              Refresh
            </button>
          </div>
        </header>

        <nav className="tabs-nav card-enter">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              className={`tab-btn ${activeTab === tab.id ? "tab-active" : ""}`}
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.label}
            </button>
          ))}
        </nav>

        {error ? <p className="error-banner">{error}</p> : null}

        {renderCurrentTab()}
      </main>

      <span className="sr-only">{clockTick}</span>
    </div>
  );
}
