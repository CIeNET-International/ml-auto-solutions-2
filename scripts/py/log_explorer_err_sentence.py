from google.cloud import logging_v2
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Set
from config_log import (
    LOGS_PROJECT_ID, 
)
import save_log_utils as utils


PAGE_SIZE = 500
KEYWORDS = ["error", "failed", "denied", "exception", "exceeded", "not found", "backoff", "unschedulable"]
#IGNORE_PATTERNS = [
    #re.compile(rf"marking task as (?:{'|'.join(re.escape(k) for k in KEYWORDS)})\. dag_id=", re.IGNORECASE),
#]
IGNORE_PATTERNS = [
    re.compile(r"marking task as failed\. dag_id=", re.IGNORECASE),
    re.compile(r"marking task as error\. dag_id=", re.IGNORECASE),
]
LOOKBACK_LINES_FOR_ECHO = 6   # inspect up to this many previous lines for an 'echo' start
CONTINUATION_MAX_LINES = 10   # when line ends with ":", append up to this many following non-empty lines
VERBOSE = False               # set True to print skip reasons (for tuning heuristics)
# ------------------------------------------

# precompiled regexes
KEYWORD_WORD_RE = re.compile(r"\b(" + "|".join(re.escape(k) for k in KEYWORDS) + r")\b", re.IGNORECASE)
IGNORE_COLON_PATTERN = re.compile(
    r"^.{0,50}:\s+(?:" + "|".join(re.escape(k) for k in KEYWORDS) + r")\s*$",
    re.IGNORECASE
)
STACK_TRACE_START = re.compile(r"Task failed with exception", re.IGNORECASE)
EXCEPTION_LINE = re.compile(r"^\s*(\w+Error|Exception|[\w\.]+):\s+(.+)$")


# --------- Helpers: echo detection & trivial checks ----------
def has_unclosed_quote_after_echo(line: str) -> bool:
    m = re.search(r"\becho\b", line, re.IGNORECASE)
    if not m:
        return False
    after = line[m.end():]
    qm = re.search(r"['\"]", after)
    if not qm:
        return False
    quote_char = qm.group(0)
    cnt = after.count(quote_char)
    return (cnt % 2) == 1


def echo_like_sh_c_unclosed(line: str) -> bool:
    l = line.lower()
    if "sh -c" in l or "bash -c" in l:
        if line.count("'") % 2 == 1 or line.count('"') % 2 == 1:
            return True
    return False


def echo_started_before_keyword(lines: List[str], keyword_lower: str, match_idx: int) -> bool:
    start = max(0, match_idx - LOOKBACK_LINES_FOR_ECHO)
    for idx in range(match_idx, start - 1, -1):
        l = lines[idx]
        ll = l.lower()
        if "echo" in ll:
            echo_pos = ll.find("echo")
            kw_pos = ll.find(keyword_lower)
            if kw_pos != -1 and echo_pos < kw_pos:
                if VERBOSE:
                    print(f"[debug] echo on same line idx={idx} before keyword -> skip")
                return True
            if has_unclosed_quote_after_echo(l):
                if idx <= match_idx:
                    if VERBOSE:
                        print(f"[debug] echo with unclosed quote at idx={idx} -> skip")
                    return True
        if "sh -c" in ll or "bash -c" in ll:
            if echo_like_sh_c_unclosed(l):
                if VERBOSE:
                    print(f"[debug] sh -c with unclosed quote at idx={idx} -> skip")
                return True
        # If we find a timestamp-like boundary, stop scanning to avoid crossing entries
        if re.match(r"^\s*\d{4}-\d{2}-\d{2}T", l):
            break
    return False


def is_trivial_sentence(sentence: str) -> bool:
    s = sentence.strip()
    # preserve continuation-introducing lines ending with ":" (do not mark trivial)
    if s.endswith(":"):
        return False
    # skip short "prefix: KEYWORD" single-line status
    if IGNORE_COLON_PATTERN.match(s):
        if VERBOSE:
            print(f"[debug] matched IGNORE_COLON_PATTERN -> trivial: {s!r}")
        return True
    # count non-numeric tokens
    words = s.split()
    non_numeric = [w for w in words if not re.fullmatch(r"[\d:.-]+", w)]
    if len(non_numeric) < 3:
        if VERBOSE:
            print(f"[debug] too few non-numeric words ({len(non_numeric)}) -> trivial: {s!r}")
        return True
    return False

# ------------------------------------------
def is_ignored_sentence(sentence: str) -> bool:
    """Return True if sentence should be ignored due to custom ignore patterns."""
    for pat in IGNORE_PATTERNS:
        if pat.search(sentence):
            if VERBOSE:
                print(f"[debug] ignored by IGNORE_PATTERNS -> {sentence!r}")
            return True
    return False

# -------------------- Core extraction ------------------------
def extract_error_messages(log_filter: str, page_size: int = PAGE_SIZE) -> Tuple[List[Dict[str, Any]], Set[str]]:
    #print(f'filter:\n{log_filter}\n{"-"*100}')
    entries = utils.query_logs(log_filter, LOGS_PROJECT_ID, PAGE_SIZE)

    results: List[Dict[str, Any]] = []
    messages = set()
    #messages: List[str] = []
    for entry in entries:
        processed_one, messages_one = process_log_entry(entry)

        if processed_one: 
            results.extend(processed_one)
        if messages_one:  
            messages.update(messages_one)

    return results, list(messages)


def process_log_entry(entry: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Set[str]]:

    results: List[Dict[str, Any]] = []
    messages = set()

    # obtain text payload
    if isinstance(entry.payload, str):
        text = entry.payload
    else:
        try:
            payload_obj = entry.payload
            if hasattr(payload_obj, "get"):
                text = payload_obj.get("message") or payload_obj.get("msg") or str(payload_obj)
            else:
                text = str(payload_obj)
        except Exception:
            text = str(entry.payload)

    severity = getattr(entry, "severity", "DEFAULT")
    timestamp = getattr(entry, "timestamp", None)
    process = getattr(entry, "process", None)
    try_number = getattr(entry, "try-number", None)
    task_id = getattr(entry, "task-id", None)
    worker_id = getattr(entry, "worker-id", None)

    ts_str = ""
    if timestamp is not None:
        try:
            ts_str = timestamp.isoformat()
        except Exception:
            ts_str = str(timestamp)

    lines = text.splitlines()
    entry_map: Dict[str, Dict[str, Any]] = {}  # sentence -> { keywords:set, sentence, severity, timestamp, text }

    i = 0
    while i < len(lines):
        line = lines[i]

        # 1) stack-trace detection
        if STACK_TRACE_START.search(line):
            j = i + 1
            stack_lines = []
            while j < len(lines) and (lines[j].startswith(" ") or lines[j].startswith("\t") or lines[j].startswith("Traceback")):
                stack_lines.append(lines[j].rstrip())
                j += 1
            root_cause = None
            for l in reversed(stack_lines):
                m = EXCEPTION_LINE.match(l)
                if m:
                    root_cause = l.strip()
                    break
            if root_cause:
                if not echo_started_before_keyword(lines, "exception", i):
                    if not is_trivial_sentence(root_cause):
                        s = root_cause
                        if s not in entry_map:
                            entry_map[s] = {
                                "keywords": set(["exception"]),
                                "sentence": s,
                                "severity": severity,
                                "timestamp": ts_str,
                                "text": text
                            }
                        else:
                            entry_map[s]["keywords"].add("exception")
            i = max(i, j - 1)
            i += 1
            continue

        # 2) keyword matches
        for kw_match in KEYWORD_WORD_RE.finditer(line):
            keyword = kw_match.group(1).lower()
            start_idx = i
            sentence = line.strip()

            # append continuation lines for ":" endings (use local pointer k; do not advance global i)
            k = start_idx
            appended = 0
            while sentence.endswith(":") and appended < CONTINUATION_MAX_LINES and (k + 1) < len(lines):
                k += 1
                appended += 1
                next_line = lines[k].strip()
                if next_line:
                    sentence += " " + next_line
                # if the appended part still endswith ":" the while continues (we already increment appended)
                if not sentence.endswith(":"):
                    break

            # echo-origin heuristic
            if echo_started_before_keyword(lines, keyword, start_idx):
                if VERBOSE:
                    print(f"[debug] skipped (echo-start) at line {start_idx}: {sentence!r}")
                continue

            # trivial skip (do not skip continuation-intro lines ending with ":")
            if is_trivial_sentence(sentence):
                if VERBOSE:
                    print(f"[debug] skipped (trivial) at line {start_idx}: {sentence!r}")
                continue

            # skip known false positives (like "marking task as FAILED. dag_id=...")
            if is_ignored_sentence(sentence):
                if VERBOSE:
                    print(f"[debug] skipped (ignored) at line {start_idx}: {sentence!r}")
                continue

            # dedupe within this entry: collect keywords for same sentence
            s = sentence
            if s not in entry_map:
                entry_map[s] = {
                    "keywords": set([keyword]),
                    "sentence": s,
                    "severity": severity,
                    "timestamp": ts_str,
                    "payload": text,
                    "process": process,
                    "task_id": task_id,
                    "try_number": try_number,
                    "worker_id": worker_id,
                }
            else:
                entry_map[s]["keywords"].add(keyword)

        i += 1

    # after processing lines for this entry, append entry_map items into global results
    for s, meta in entry_map.items():
        if sentence not in messages:
          results.append({
            "timestamp": meta["timestamp"],
            "severity": meta["severity"],
            "keywords": sorted(meta["keywords"]),
            "sentence": meta["sentence"],
            "payload": meta["payload"],
            "process": meta["process"],
            "task_id": meta["task_id"],
            "try_number": meta["try_number"],
            "worker_id": meta["worker_id"],
          })
          messages.add(meta["sentence"])

    return results, messages


FILTER = f'''
resource.type="cloud_composer_environment"
resource.labels.environment_name="ml-automation-solutions"
labels.workflow="new_internal_stable_release_a3ultra_llama3.1-405b_256gpus_fp8_maxtext"
labels.execution-date="2025-08-23T08:30:00+00:00"
labels.task-id=~"^run_internal_dag_united_workload.*"
timestamp >= "2025-08-26T08:30:26.987000+00:00"
timestamp <= "2025-08-26T08:34:34.581000+00:00"
(
  textPayload =~ "(?i)error|failed|exception|denied|exceeded|not found|backoff|unschedulable"
  OR jsonPayload.message =~ "(?i)error|failed|exception|denied|exceeded|not found|backoff|unschedulable"
)
severity = INFO OR severity = NOTICE OR severity = WARNING
'''

# -------------------- Run & pretty-print -----------------------
if __name__ == "__main__":
    found, messages = extract_error_messages(FILTER, page_size=PAGE_SIZE)
    if not found:
        print("No valid error messages found.")
    else:
        for m in found:
            kws = ", ".join(m["keywords"])
            print(f"[{m['timestamp']}]{m['severity']}  Matched keywords: {kws}")
            print(f"Sentence: {m['sentence']}\n{'-'*100}")
            print(f"{m['payload']}\n{'='*150}")

    print(f'messages:{messages}')

