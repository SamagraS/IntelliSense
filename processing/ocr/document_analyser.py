"""
document_analyzers.py
=====================
Rule-based analysis modules for normalized OCR output.

Each function takes structured rows (List[dict]) and/or raw text lines (List[str])
and returns a typed result dict.  No ML — pure regex + arithmetic rules.

Usage
-----
    from document_analyzers import (
        analyze_alm,
        analyze_shareholding,
        analyze_borrowing_profile,
        analyze_portfolio_cuts,
        analyze_board_minutes,
        analyze_sanction_letter,
        analyze_rating_report,
    )
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PRIME_RATE_PCT: float = 9.0          # RBI repo-linked benchmark; update periodically
HIGH_INTEREST_SPREAD: float = 5.0    # flag if rate > PRIME_RATE + this spread

PLEDGE_CAUTION_THRESHOLD: float = 40.0
PLEDGE_HIGH_RISK_THRESHOLD: float = 60.0

SHORT_TERM_DAYS: int = 30            # buckets < 30 days are "short-term" for ALM flags

RESTRUCTURING_KEYWORDS: list[str] = [
    "ots", "one time settlement", "waiver", "moratorium",
    "restructuring", "restructured", "npa", "rescheduled",
    "write-off", "write off",
]

GOVERNANCE_KEYWORDS: dict[str, str] = {
    "related_party_transaction":  r"related[\s\-]party\s+transaction",
    "auditor_resignation":        r"auditor[\s\w]*resign",
    "loan_approval":              r"loan\s+approv",
    "waiver":                     r"\bwaiver\b",
    "debt_restructuring":         r"debt[\s\-]restructur",
    "director_loan":              r"director[\s\w]*loan|loan[\s\w]*director",
}

RATING_GRADES: list[str] = [
    "AAA", "AA+", "AA", "AA-",
    "A+",  "A",   "A-",
    "BBB+","BBB", "BBB-",
    "BB+", "BB",  "BB-",
    "B+",  "B",   "B-",
    "C",   "D",
]

COVENANT_TRIGGER_WORDS: list[str] = [
    "debt service coverage", "dscr", "leverage ratio", "current ratio",
    "net worth", "npa", "promoter holding", "debt equity", "ltv",
    "interest coverage", "total indebtedness",
]


# ---------------------------------------------------------------------------
# Shared regex helpers
# ---------------------------------------------------------------------------

def _find_percentage(text: str) -> list[float]:
    """Return all percentage values found in *text* as floats."""
    return [float(m) for m in re.findall(r"(\d+(?:\.\d+)?)\s*%", text)]


def _find_first_percentage(text: str) -> float | None:
    hits = _find_percentage(text)
    return hits[0] if hits else None


def _find_amount_inr(text: str) -> float | None:
    """
    Extract a currency amount from text.
    Handles: ₹ 50 Cr, INR 500 lakh, Rs. 10,00,000, 250.5 crore
    Returns amount normalised to INR (crore × 1e7, lakh × 1e5).
    """
    # Crore
    m = re.search(
        r"(?:₹|rs\.?|inr)?\s*(\d[\d,]*(?:\.\d+)?)\s*(?:cr(?:ore)?s?)",
        text, re.IGNORECASE,
    )
    if m:
        return _parse_number(m.group(1)) * 1e7

    # Lakh
    m = re.search(
        r"(?:₹|rs\.?|inr)?\s*(\d[\d,]*(?:\.\d+)?)\s*(?:lakh|lac)s?",
        text, re.IGNORECASE,
    )
    if m:
        return _parse_number(m.group(1)) * 1e5

    # Plain number with currency prefix
    m = re.search(
        r"(?:₹|rs\.?|inr)\s*(\d[\d,]*(?:\.\d+)?)",
        text, re.IGNORECASE,
    )
    if m:
        return _parse_number(m.group(1))

    return None


def _parse_number(raw: str) -> float:
    """Strip Indian-format commas and cast to float."""
    return float(raw.replace(",", "").strip())


def _parse_maturity_days(bucket_label: str) -> int | None:
    """
    Convert a maturity bucket label to its upper-bound in days.
    Handles: '1-7 days', '7-14 days', '1 month', '3 months',
             '6 months', '1 year', '1-3 years', 'over 3 years'.
    Returns None if unparseable (treated as long-term).
    """
    label = bucket_label.lower().strip()

    # Explicit day ranges: "1-7 days", "7 days", "up to 30 days"
    m = re.search(r"(\d+)\s*(?:to|-)\s*(\d+)\s*day", label)
    if m:
        return int(m.group(2))
    m = re.search(r"(\d+)\s*day", label)
    if m:
        return int(m.group(1))

    # Month-based: "1 month", "2-3 months"
    m = re.search(r"(\d+)\s*(?:to|-)\s*(\d+)\s*month", label)
    if m:
        return int(m.group(2)) * 30
    m = re.search(r"(\d+)\s*month", label)
    if m:
        return int(m.group(1)) * 30

    # Year-based: "1 year", "1-3 years", "over 3 years"
    m = re.search(r"over\s+(\d+)\s*year", label)
    if m:
        return int(m.group(1)) * 365 + 1          # "over N years" → N+ε
    m = re.search(r"(\d+)\s*(?:to|-)\s*(\d+)\s*year", label)
    if m:
        return int(m.group(2)) * 365
    m = re.search(r"(\d+)\s*year", label)
    if m:
        return int(m.group(1)) * 365

    return None


def _context_window(lines: list[str], keyword: str, window: int = 3) -> list[str]:
    """Return *window* lines before+after any line containing *keyword*."""
    snippets: list[str] = []
    for i, line in enumerate(lines):
        if keyword.lower() in line.lower():
            start = max(0, i - window)
            end = min(len(lines), i + window + 1)
            snippets.append(" | ".join(lines[start:end]))
    return snippets


def _clean(text: str) -> str:
    """Collapse whitespace and strip."""
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# 1. ALM Analysis
# ---------------------------------------------------------------------------

def analyze_alm(alm_rows: list[dict]) -> dict:
    """
    Compute gap and cumulative gap for ALM maturity buckets.

    Expected row keys (case-insensitive, flexible naming):
        maturity_bucket       : str   e.g. "1-7 days", "1 month"
        assets_bucket_inr     : float INR amount
        liabilities_bucket_inr: float INR amount

    Returns
    -------
    {
        "enriched_rows": [...],   # original rows + gap + cumulative_gap + days_upper
        "short_term_flags": [...] # rows with maturity <30d AND negative gap
        "summary": {
            "total_assets_inr": float,
            "total_liabilities_inr": float,
            "net_gap_inr": float,
            "worst_bucket": str | None,
        }
    }
    """
    def _get(row: dict, *keys: str) -> Any:
        for k in keys:
            for rk in row:
                if rk.lower().replace(" ", "_") == k.lower():
                    return row[rk]
        return None

    enriched: list[dict] = []
    cumulative = 0.0
    flags: list[dict] = []

    # Sort rows by parsed maturity upper-bound; unparseable → infinity (long-term)
    def sort_key(row: dict) -> int:
        label = str(_get(row, "maturity_bucket") or "")
        days = _parse_maturity_days(label)
        return days if days is not None else 999_999

    sorted_rows = sorted(alm_rows, key=sort_key)

    total_assets = 0.0
    total_liabilities = 0.0
    worst_gap: float = 0.0
    worst_bucket: str | None = None

    for row in sorted_rows:
        bucket = str(_get(row, "maturity_bucket") or "unknown")
        try:
            assets = float(_get(row, "assets_bucket_inr") or 0)
        except (TypeError, ValueError):
            assets = 0.0
        try:
            liabilities = float(_get(row, "liabilities_bucket_inr") or 0)
        except (TypeError, ValueError):
            liabilities = 0.0

        gap = assets - liabilities
        cumulative += gap
        days_upper = _parse_maturity_days(bucket)

        total_assets += assets
        total_liabilities += liabilities

        enriched_row = {
            **row,
            "gap_inr": round(gap, 2),
            "cumulative_gap_inr": round(cumulative, 2),
            "days_upper_bound": days_upper,
        }
        enriched.append(enriched_row)

        # Flag negative gaps in short-term buckets
        if gap < 0 and days_upper is not None and days_upper < SHORT_TERM_DAYS:
            flags.append({
                "bucket": bucket,
                "gap_inr": round(gap, 2),
                "days_upper": days_upper,
                "severity": "critical" if gap < -1_000_000 else "warning",
            })

        if gap < worst_gap:
            worst_gap = gap
            worst_bucket = bucket

    return {
        "enriched_rows": enriched,
        "short_term_flags": flags,
        "summary": {
            "total_assets_inr": round(total_assets, 2),
            "total_liabilities_inr": round(total_liabilities, 2),
            "net_gap_inr": round(total_assets - total_liabilities, 2),
            "worst_bucket": worst_bucket,
            "worst_gap_inr": round(worst_gap, 2),
        },
    }


# ---------------------------------------------------------------------------
# 2. Shareholding Analysis
# ---------------------------------------------------------------------------

def analyze_shareholding(text_lines: list[str]) -> dict:
    """
    Extract promoter pledge % from OCR text and classify risk.

    Looks for patterns like:
        "Promoter shares pledged: 45.3%"
        "pledged 52% of promoter holding"
        "promoter pledge ratio: 38%"

    Returns
    -------
    {
        "promoter_pledge_pct": float | None,
        "risk_tag": "normal" | "caution" | "high_risk" | "unknown",
        "source_line": str | None,
        "all_pledge_mentions": [{"line": str, "pct": float}]
    }
    """
    # Patterns ordered by specificity
    PLEDGE_PATTERNS: list[re.Pattern] = [
        # "pledged 45.3% of promoter"
        re.compile(
            r"pledge[d]?\s+(\d+(?:\.\d+)?)\s*%\s*(?:of\s+)?(?:promoter|their)",
            re.IGNORECASE,
        ),
        # "promoter.*pledge.*45.3%"
        re.compile(
            r"promoter.*?pledge[d]?.*?(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE,
        ),
        # "pledge ratio.*45%"
        re.compile(
            r"pledge[d]?\s*(?:ratio|percentage|%)?\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*%",
            re.IGNORECASE,
        ),
        # "45% pledged"
        re.compile(
            r"(\d+(?:\.\d+)?)\s*%\s+pledge[d]?",
            re.IGNORECASE,
        ),
    ]

    all_mentions: list[dict] = []

    for line in text_lines:
        for pat in PLEDGE_PATTERNS:
            m = pat.search(line)
            if m:
                pct = float(m.group(1))
                all_mentions.append({"line": _clean(line), "pct": pct})
                break   # one mention per line

    if not all_mentions:
        return {
            "promoter_pledge_pct": None,
            "risk_tag": "unknown",
            "source_line": None,
            "all_pledge_mentions": [],
        }

    # Use the highest pledge % found (most conservative interpretation)
    best = max(all_mentions, key=lambda x: x["pct"])
    pct = best["pct"]

    if pct >= PLEDGE_HIGH_RISK_THRESHOLD:
        tag = "high_risk"
    elif pct >= PLEDGE_CAUTION_THRESHOLD:
        tag = "caution"
    else:
        tag = "normal"

    return {
        "promoter_pledge_pct": pct,
        "risk_tag": tag,
        "source_line": best["line"],
        "all_pledge_mentions": all_mentions,
    }


# ---------------------------------------------------------------------------
# 3. Borrowing Profile Analysis
# ---------------------------------------------------------------------------

def analyze_borrowing_profile(
    rows: list[dict],
    context_text: list[str],
) -> dict:
    """
    Flag restructured and high-interest borrowings.

    Row keys expected:
        lender_name        : str
        amount_in_inr      : float   (can also be amount_inr / amount)
        interest_rate_pct  : float

    context_text: full OCR text lines from the same document, used to
    detect restructuring keywords appearing near any lender name.

    Returns
    -------
    {
        "flagged_rows": [
            {
                "lender_name": str,
                "amount_in_inr": float,
                "interest_rate_pct": float,
                "flags": ["restructuring" | "high_interest"],
                "context_snippets": [str],
            }
        ],
        "summary": {
            "total_borrowings_inr": float,
            "restructured_count": int,
            "high_interest_count": int,
            "restructured_amount_inr": float,
            "high_interest_amount_inr": float,
        }
    }
    """
    HIGH_THRESHOLD = PRIME_RATE_PCT + HIGH_INTEREST_SPREAD
    full_text = " ".join(context_text).lower()

    flagged: list[dict] = []
    total_inr = 0.0
    restructured_inr = 0.0
    high_interest_inr = 0.0
    restructured_count = 0
    high_interest_count = 0

    def _get_field(row: dict, *candidates: str) -> Any:
        """Flexible key lookup across naming variants."""
        for c in candidates:
            for k in row:
                if k.lower().replace(" ", "_").replace("-", "_") == c:
                    return row[k]
        return None

    for row in rows:
        lender = str(_get_field(row, "lender_name", "lender", "bank", "institution") or "unknown")
        try:
            amount = float(
                _get_field(row, "amount_in_inr", "amount_inr", "amount", "outstanding_inr") or 0
            )
        except (TypeError, ValueError):
            amount = 0.0
        try:
            rate = float(
                _get_field(row, "interest_rate_pct", "interest_rate", "rate_pct", "rate") or 0
            )
        except (TypeError, ValueError):
            rate = 0.0

        total_inr += amount
        row_flags: list[str] = []
        snippets: list[str] = []

        # --- Restructuring check ---
        # 1. Keywords anywhere near the lender name in context_text
        lender_context = _context_window(context_text, lender, window=5)
        for snippet in lender_context:
            for kw in RESTRUCTURING_KEYWORDS:
                if kw in snippet.lower():
                    row_flags.append("restructuring")
                    snippets.append(_clean(snippet))
                    break

        # 2. Also scan the row's own string values for keywords
        row_text = " ".join(str(v) for v in row.values()).lower()
        for kw in RESTRUCTURING_KEYWORDS:
            if kw in row_text and "restructuring" not in row_flags:
                row_flags.append("restructuring")
                snippets.append(f"[row value] {row_text[:120]}")
                break

        # --- High interest check ---
        if rate > HIGH_THRESHOLD:
            row_flags.append("high_interest")

        if "restructuring" in row_flags:
            restructured_count += 1
            restructured_inr += amount
        if "high_interest" in row_flags:
            high_interest_count += 1
            high_interest_inr += amount

        if row_flags:
            flagged.append({
                "lender_name": lender,
                "amount_in_inr": round(amount, 2),
                "interest_rate_pct": rate,
                "flags": sorted(set(row_flags)),
                "context_snippets": list(dict.fromkeys(snippets)),  # deduplicate, preserve order
            })

    return {
        "flagged_rows": flagged,
        "summary": {
            "total_borrowings_inr": round(total_inr, 2),
            "restructured_count": restructured_count,
            "high_interest_count": high_interest_count,
            "restructured_amount_inr": round(restructured_inr, 2),
            "high_interest_amount_inr": round(high_interest_inr, 2),
            "prime_rate_used_pct": PRIME_RATE_PCT,
            "high_interest_threshold_pct": HIGH_THRESHOLD,
        },
    }


# ---------------------------------------------------------------------------
# 4. Portfolio Cuts Analysis
# ---------------------------------------------------------------------------

def analyze_portfolio_cuts(rows: list[dict]) -> dict:
    """
    Extract key NPA / concentration / coverage metrics from portfolio rows.

    Flexible input: rows may contain the metrics directly as column values
    OR they may need to be found by scanning for known label strings.

    Expected column names (any case, partial match accepted):
        gnpa_pct / gross_npa / gross npa %
        nnpa_pct / net_npa   / net npa %
        top10_concentration_pct / top 10 / concentration
        provision_coverage_ratio / pcr / provision coverage

    Returns
    -------
    {
        "gnpa_pct": float | None,
        "nnpa_pct": float | None,
        "top10_concentration_pct": float | None,
        "provision_coverage_ratio": float | None,
        "flags": ["high_gnpa" | "high_concentration" | "low_pcr"],
        "row_count": int,
        "source_rows": { metric: row_index }
    }
    """
    # Column alias map: canonical_name → list of partial match strings
    ALIASES: dict[str, list[str]] = {
        "gnpa_pct":                ["gnpa", "gross_npa", "gross npa"],
        "nnpa_pct":                ["nnpa", "net_npa", "net npa"],
        "top10_concentration_pct": ["top10", "top 10", "concentration", "top_10"],
        "provision_coverage_ratio":["pcr", "provision_coverage", "provision coverage"],
    }

    def _find_col(row: dict, aliases: list[str]) -> str | None:
        for col in row:
            col_norm = col.lower().replace("-", " ").replace("_", " ")
            for alias in aliases:
                if alias.replace("_", " ") in col_norm:
                    return col
        return None

    def _to_float(val: Any) -> float | None:
        if val is None:
            return None
        try:
            s = str(val).replace("%", "").replace(",", "").strip()
            return float(s) if s else None
        except ValueError:
            return None

    metrics: dict[str, float | None] = {k: None for k in ALIASES}
    source_rows: dict[str, int] = {}

    for idx, row in enumerate(rows):
        for metric, aliases in ALIASES.items():
            if metrics[metric] is not None:
                continue
            col = _find_col(row, aliases)
            if col is not None:
                val = _to_float(row[col])
                if val is not None:
                    metrics[metric] = val
                    source_rows[metric] = idx
                    continue

            # Fallback: if a "label" column contains the alias, take the "value" column
            label_col = _find_col(row, ["label", "particular", "metric", "description"])
            value_col = _find_col(row, ["value", "amount", "data", "%"])
            if label_col and value_col:
                cell = str(row.get(label_col, "")).lower()
                for alias in aliases:
                    if alias.replace("_", " ") in cell:
                        val = _to_float(row.get(value_col))
                        if val is not None:
                            metrics[metric] = val
                            source_rows[metric] = idx

    flags: list[str] = []
    if metrics["gnpa_pct"] is not None and metrics["gnpa_pct"] > 5.0:
        flags.append("high_gnpa")
    if metrics["top10_concentration_pct"] is not None and metrics["top10_concentration_pct"] > 30.0:
        flags.append("high_concentration")
    if metrics["provision_coverage_ratio"] is not None and metrics["provision_coverage_ratio"] < 50.0:
        flags.append("low_pcr")

    return {
        **metrics,
        "flags": flags,
        "row_count": len(rows),
        "source_rows": source_rows,
    }


# ---------------------------------------------------------------------------
# 5. Board Minutes Analysis
# ---------------------------------------------------------------------------

def analyze_board_minutes(text_lines: list[str]) -> dict:
    """
    Scan board meeting text for governance red flags.

    Returns
    -------
    {
        "governance_signals": [
            {
                "type": str,
                "snippet": str,
                "line_index": int
            }
        ],
        "signal_counts": { type: count },
        "overall_risk": "low" | "medium" | "high"
    }
    """
    signals: list[dict] = []

    for i, line in enumerate(text_lines):
        line_lower = line.lower()
        for signal_type, pattern in GOVERNANCE_KEYWORDS.items():
            if re.search(pattern, line_lower):
                start = max(0, i - 1)
                end = min(len(text_lines), i + 3)
                snippet = _clean(" | ".join(text_lines[start:end]))
                signals.append({
                    "type": signal_type,
                    "snippet": snippet[:400],   # cap snippet length
                    "line_index": i,
                })
                # Don't break — one line may match multiple governance types

    # Deduplicate: same type+line
    seen: set[tuple[str, int]] = set()
    unique: list[dict] = []
    for s in signals:
        key = (s["type"], s["line_index"])
        if key not in seen:
            seen.add(key)
            unique.append(s)

    signal_counts: dict[str, int] = {}
    for s in unique:
        signal_counts[s["type"]] = signal_counts.get(s["type"], 0) + 1

    # Risk escalation rules
    high_risk_types = {"auditor_resignation", "debt_restructuring", "director_loan"}
    medium_risk_types = {"related_party_transaction", "waiver", "loan_approval"}

    if any(t in signal_counts for t in high_risk_types):
        overall_risk = "high"
    elif any(t in signal_counts for t in medium_risk_types):
        overall_risk = "medium"
    elif unique:
        overall_risk = "medium"
    else:
        overall_risk = "low"

    return {
        "governance_signals": unique,
        "signal_counts": signal_counts,
        "overall_risk": overall_risk,
    }


# ---------------------------------------------------------------------------
# 6. Sanction Letter Analysis
# ---------------------------------------------------------------------------

def analyze_sanction_letter(text_lines: list[str]) -> dict:
    """
    Extract key terms from a bank sanction letter.

    Returns
    -------
    {
        "loan_amount_inr": float | None,
        "interest_rate_pct": float | None,
        "tenor": str | None,
        "collateral_description": str | None,
        "covenant_clauses": [str],
        "restructuring_mentions": [str],
        "default_mentions": [str],
        "flags": ["restructuring_present" | "default_mentioned" | "high_interest"]
    }
    """
    full_text = " ".join(text_lines)
    flags: list[str] = []

    # ---- Loan amount ----
    loan_amount = None
    for line in text_lines:
        if re.search(r"\b(sanction|loan|facility|limit)\b", line, re.IGNORECASE):
            amt = _find_amount_inr(line)
            if amt:
                loan_amount = amt
                break

    # ---- Interest rate ----
    interest_rate = None
    rate_patterns = [
        r"interest\s+rate\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*%",
        r"rate\s+of\s+interest\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*%",
        r"roi\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*p\.?a\.?\b",
    ]
    for pat in rate_patterns:
        m = re.search(pat, full_text, re.IGNORECASE)
        if m:
            interest_rate = float(m.group(1))
            break

    if interest_rate and interest_rate > PRIME_RATE_PCT + HIGH_INTEREST_SPREAD:
        flags.append("high_interest")

    # ---- Tenor ----
    tenor = None
    tenor_patterns = [
        r"tenor\s*[:\-]?\s*([\d]+\s+(?:months?|years?|days?))",
        r"tenure\s*[:\-]?\s*([\d]+\s+(?:months?|years?|days?))",
        r"repayment\s+period\s*[:\-]?\s*([\d]+\s+(?:months?|years?|days?))",
        r"loan\s+period\s*[:\-]?\s*([\d]+\s+(?:months?|years?|days?))",
    ]
    for pat in tenor_patterns:
        m = re.search(pat, full_text, re.IGNORECASE)
        if m:
            tenor = _clean(m.group(1))
            break

    # ---- Collateral ----
    collateral = None
    collateral_patterns = [
        r"(?:collateral|security|mortgage|hypothecation|pledge)[d]?\s*[:\-]?\s*([^.\n]{10,150})",
        r"secured\s+by\s+([^.\n]{10,150})",
        r"charge\s+on\s+([^.\n]{10,150})",
    ]
    for pat in collateral_patterns:
        m = re.search(pat, full_text, re.IGNORECASE)
        if m:
            collateral = _clean(m.group(1))
            break

    # ---- Covenants ----
    covenant_clauses: list[str] = []
    for line in text_lines:
        line_lower = line.lower()
        for trigger in COVENANT_TRIGGER_WORDS:
            if trigger in line_lower:
                covenant_clauses.append(_clean(line)[:250])
                break  # one match per line

    # ---- Restructuring mentions ----
    restructuring_mentions: list[str] = []
    for line in text_lines:
        line_lower = line.lower()
        for kw in ["restructuring", "moratorium", "waiver", "ots", "rescheduled"]:
            if kw in line_lower:
                restructuring_mentions.append(_clean(line)[:250])
                break

    if restructuring_mentions:
        flags.append("restructuring_present")

    # ---- Default mentions ----
    default_mentions: list[str] = []
    default_keywords = ["default", "npa", "overdue", "non-performing", "breach of covenant"]
    for line in text_lines:
        line_lower = line.lower()
        for kw in default_keywords:
            if kw in line_lower:
                default_mentions.append(_clean(line)[:250])
                break

    if default_mentions:
        flags.append("default_mentioned")

    return {
        "loan_amount_inr": loan_amount,
        "interest_rate_pct": interest_rate,
        "tenor": tenor,
        "collateral_description": collateral,
        "covenant_clauses": list(dict.fromkeys(covenant_clauses)),   # deduplicate
        "restructuring_mentions": restructuring_mentions,
        "default_mentions": default_mentions,
        "flags": flags,
    }


# ---------------------------------------------------------------------------
# 7. Rating Report Analysis
# ---------------------------------------------------------------------------

def analyze_rating_report(text_lines: list[str]) -> dict:
    """
    Extract credit rating, outlook, rating history, and risk factors.

    Returns
    -------
    {
        "current_rating": str | None,
        "outlook": "stable" | "negative" | "positive" | "watch" | None,
        "last_change": {
            "date": str | None,
            "direction": "upgrade" | "downgrade" | "reaffirm" | None,
            "from_rating": str | None,
            "to_rating": str | None
        },
        "rating_agency": str | None,
        "key_risk_factors": [str],
        "flags": ["negative_outlook" | "recent_downgrade" | "watch_listed"]
    }
    """
    full_text = " ".join(text_lines)
    flags: list[str] = []

    # ---- Rating pattern ----
    # Matches: "rated BBB+", "rating: A-", "assigned AA (Stable)"
    RATING_RE = re.compile(
        r"\b(?:rated?|rating|assigned|reaffirm(?:ed)?|upgraded?\s+to|downgraded?\s+to)\b"
        r"[\s:\-]*"
        r"([A-D]{1,3}[+\-]?)"
        r"(?:\s*\(([^)]+)\))?",
        re.IGNORECASE,
    )
    STANDALONE_RATING_RE = re.compile(
        r"(?<!\w)([A-D]{1,3}[+\-]?)(?!\w)",
    )

    current_rating: str | None = None
    outlook_from_bracket: str | None = None

    m = RATING_RE.search(full_text)
    if m:
        candidate = m.group(1).upper()
        if candidate in RATING_GRADES:
            current_rating = candidate
        if m.group(2):
            outlook_from_bracket = m.group(2).strip().lower()

    # If not found via prefix, scan for standalone ratings near rating-related words
    if current_rating is None:
        for line in text_lines:
            if re.search(r"\brating\b", line, re.IGNORECASE):
                for m2 in STANDALONE_RATING_RE.finditer(line):
                    candidate = m2.group(1).upper()
                    if candidate in RATING_GRADES:
                        current_rating = candidate
                        break
            if current_rating:
                break

    # ---- Outlook ----
    outlook: str | None = None
    OUTLOOK_PATTERNS = [
        (r"\bstable\b",           "stable"),
        (r"\bnegative\b",         "negative"),
        (r"\bpositive\b",         "positive"),
        (r"\bwatch\b",            "watch"),
        (r"\bcreditwatch\b",      "watch"),
        (r"\brating\s+watch\b",   "watch"),
    ]
    for pat, label in OUTLOOK_PATTERNS:
        if re.search(pat, full_text, re.IGNORECASE):
            outlook = label
            break

    # Prefer outlook extracted from bracket "(Stable)" over full-text scan
    if outlook_from_bracket:
        for pat, label in OUTLOOK_PATTERNS:
            if re.search(pat, outlook_from_bracket, re.IGNORECASE):
                outlook = label
                break

    if outlook == "negative":
        flags.append("negative_outlook")
    if outlook == "watch":
        flags.append("watch_listed")

    # ---- Rating change / history ----
    direction: str | None = None
    from_rating: str | None = None
    to_rating: str | None = None
    change_date: str | None = None

    DOWNGRADE_RE = re.compile(
        r"downgrad(?:ed?)?\s+(?:[\w\s,\.]+?\s+)?from\s+([A-D]{1,3}[+\-]?)\s+to\s+([A-D]{1,3}[+\-]?)",
        re.IGNORECASE,
    )
    UPGRADE_RE = re.compile(
        r"upgrad(?:ed?)?\s+(?:[\w\s,\.]+?\s+)?from\s+([A-D]{1,3}[+\-]?)\s+to\s+([A-D]{1,3}[+\-]?)",
        re.IGNORECASE,
    )
    REAFFIRM_RE = re.compile(r"reaffirm|affirm|maintain|unchanged", re.IGNORECASE)

    DATE_RE = re.compile(
        r"\b(\d{1,2}[\s\-/](?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"
        r"[\s\-/]\d{2,4}"
        r"|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[\s\-,]+\d{4}"
        r"|\d{4})\b",
        re.IGNORECASE,
    )

    for line in text_lines:
        dg = DOWNGRADE_RE.search(line)
        if dg:
            direction = "downgrade"
            from_rating = dg.group(1).upper()
            to_rating = dg.group(2).upper()
            dm = DATE_RE.search(line)
            change_date = dm.group(0) if dm else None
            flags.append("recent_downgrade")
            break

        ug = UPGRADE_RE.search(line)
        if ug:
            direction = "upgrade"
            from_rating = ug.group(1).upper()
            to_rating = ug.group(2).upper()
            dm = DATE_RE.search(line)
            change_date = dm.group(0) if dm else None
            break

        if REAFFIRM_RE.search(line) and current_rating:
            direction = "reaffirm"
            dm = DATE_RE.search(line)
            change_date = dm.group(0) if dm else None

    # ---- Rating agency ----
    AGENCY_RE = re.compile(
        r"\b(CRISIL|ICRA|CARE|India\s+Ratings?|Brickwork|Acuité|Infomerics|Fitch|Moody'?s|S&P)\b",
        re.IGNORECASE,
    )
    agency = None
    m_agency = AGENCY_RE.search(full_text)
    if m_agency:
        agency = _clean(m_agency.group(1))

    # ---- Key risk factors ----
    RISK_TRIGGER_PHRASES = [
        "key risk", "risk factor", "concern", "challenge", "weakness",
        "vulnerability", "exposure", "concentration risk", "asset quality",
        "npa", "liquidity risk", "refinancing risk",
    ]
    key_risks: list[str] = []
    for line in text_lines:
        line_lower = line.lower()
        for phrase in RISK_TRIGGER_PHRASES:
            if phrase in line_lower:
                key_risks.append(_clean(line)[:300])
                break

    # Deduplicate while preserving order
    key_risks = list(dict.fromkeys(key_risks))

    return {
        "current_rating": current_rating,
        "outlook": outlook,
        "last_change": {
            "date": change_date,
            "direction": direction,
            "from_rating": from_rating,
            "to_rating": to_rating,
        },
        "rating_agency": agency,
        "key_risk_factors": key_risks,
        "flags": flags,
    }


# ---------------------------------------------------------------------------
# Quick smoke-test (run directly: python document_analyzers.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    # --- ALM ---
    alm_sample = [
        {"maturity_bucket": "1-7 days",   "assets_bucket_inr": 500_000, "liabilities_bucket_inr": 800_000},
        {"maturity_bucket": "8-14 days",  "assets_bucket_inr": 300_000, "liabilities_bucket_inr": 200_000},
        {"maturity_bucket": "15-30 days", "assets_bucket_inr": 900_000, "liabilities_bucket_inr": 700_000},
        {"maturity_bucket": "1-3 months", "assets_bucket_inr": 2_000_000, "liabilities_bucket_inr": 1_500_000},
        {"maturity_bucket": "1-3 years",  "assets_bucket_inr": 5_000_000, "liabilities_bucket_inr": 3_000_000},
    ]
    print("=== ALM ===")
    print(json.dumps(analyze_alm(alm_sample), indent=2))

    # --- Shareholding ---
    sh_lines = [
        "The promoter holding as of September 2023 stands at 62.4%.",
        "Promoter shares pledged: 45.3% of total promoter holding.",
        "This represents an increase from the previous quarter's pledge of 38%.",
    ]
    print("\n=== SHAREHOLDING ===")
    print(json.dumps(analyze_shareholding(sh_lines), indent=2))

    # --- Borrowing Profile ---
    bp_rows = [
        {"lender_name": "SBI", "amount_in_inr": 50_000_000, "interest_rate_pct": 9.5},
        {"lender_name": "HDFC Bank", "amount_in_inr": 30_000_000, "interest_rate_pct": 15.5},
        {"lender_name": "IndusInd", "amount_in_inr": 20_000_000, "interest_rate_pct": 11.0,
         "notes": "OTS settlement agreed for this tranche"},
    ]
    bp_context = [
        "The IndusInd loan was restructured under OTS scheme in FY23.",
        "SBI facility continues at standard rates.",
        "HDFC Bank charges higher ROI due to risk premium.",
    ]
    print("\n=== BORROWING PROFILE ===")
    print(json.dumps(analyze_borrowing_profile(bp_rows, bp_context), indent=2))

    # --- Portfolio Cuts ---
    pf_rows = [
        {"label": "Gross NPA %",              "value": "7.2%"},
        {"label": "Net NPA %",                "value": "3.1%"},
        {"label": "Top 10 Concentration %",   "value": "35.0%"},
        {"label": "Provision Coverage Ratio", "value": "42.5%"},
    ]
    print("\n=== PORTFOLIO CUTS ===")
    print(json.dumps(analyze_portfolio_cuts(pf_rows), indent=2))

    # --- Board Minutes ---
    bm_lines = [
        "The Board approved the related party transaction with ABC Infra Ltd.",
        "Item 5: Loan approval of Rs 50 Cr to subsidiary was discussed.",
        "The auditor has tendered their resignation effective immediately.",
        "Debt restructuring plan presented for the retail portfolio.",
        "No other business was discussed.",
    ]
    print("\n=== BOARD MINUTES ===")
    print(json.dumps(analyze_board_minutes(bm_lines), indent=2))

    # --- Sanction Letter ---
    sl_lines = [
        "Sanction of term loan of Rs. 25 Crores to M/s ABC Housing Finance Ltd.",
        "Rate of Interest: 12.5% p.a. linked to MCLR.",
        "Tenor: 84 months from first disbursement.",
        "Security: Mortgage of residential properties, hypothecation of receivables.",
        "Covenant: DSCR to be maintained above 1.25x at all times.",
        "Covenant: Debt equity ratio not to exceed 7:1.",
        "The facility includes a moratorium of 6 months on principal.",
    ]
    print("\n=== SANCTION LETTER ===")
    print(json.dumps(analyze_sanction_letter(sl_lines), indent=2))

    # --- Rating Report ---
    rr_lines = [
        "CRISIL has downgraded the rating of ABC HFC from BBB+ to BBB- in September 2023.",
        "Outlook: Negative.",
        "Key risk factors include deteriorating asset quality and high NPA levels.",
        "Concentration risk in the affordable housing segment remains a concern.",
        "Liquidity risk is elevated due to ALM mismatch.",
    ]
    print("\n=== RATING REPORT ===")
    print(json.dumps(analyze_rating_report(rr_lines), indent=2))