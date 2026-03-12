"""
gst_bank_recon.py
=================
GST-vs-Bank reconciliation engine for credit risk analysis.

For each calendar month, the engine cross-checks three data sources:

  1. GSTR-3B  — self-declared sales and purchase figures.
  2. GSTR-2A  — supplier-reported purchase figures (auto-populated by GSTN).
  3. Bank statements — actual money flows through the current/OD account.

Three fraud / risk signals are detected per month
--------------------------------------------------
  REVENUE_INFLATION
      Declared GST sales are materially higher than actual bank credits,
      suggesting the entity is overstating turnover to appear creditworthy.

  GSTR_2A_3B_MISMATCH
      The ITC (Input Tax Credit) the entity claims in 3B is significantly
      higher than what its suppliers actually reported in 2A, which can
      indicate fake invoicing or round-tripping of input credits.

  CIRCULAR_TRADING_SUSPECTED
      Large, round-number credits are followed within a few days by
      near-identical debits — a heuristic for money going out the back door
      after being brought in to inflate visible bank turnover.

Usage
-----
    from gst_bank_recon import run_gst_bank_recon, GSTR3BMonth, GSTR2AMonth, BankTransaction
    from datetime import date

    result = run_gst_bank_recon(
        company_id="COMP_001",
        gstr3b=[GSTR3BMonth(month=date(2024, 1, 1), declared_sales=5_00_000, declared_purchases=3_00_000)],
        gstr2a=[GSTR2AMonth(month=date(2024, 1, 1), supplier_reported_purchases=2_50_000)],
        txns=[BankTransaction(date=date(2024, 1, 5), amount=4_20_000, type="credit", narration="NEFT recv")],
    )
    print(result.revenue_reliability_score)
"""

from __future__ import annotations

import itertools
from collections import defaultdict
from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Thresholds & scoring constants
# (override by passing a ReconConfig to run_gst_bank_recon)
# ---------------------------------------------------------------------------

# Fraction by which declared_sales may exceed bank credits before the month
# is flagged for revenue inflation.  0.30 = 30 %.
DEFAULT_INFLATION_THRESHOLD: float = 0.30

# Absolute rupee difference between declared and supplier-reported purchases
# that triggers a 2A/3B mismatch flag.  10 000 = ₹10,000.
DEFAULT_MISMATCH_ABS_THRESHOLD: float = 10_000.0

# Fraction of declared purchases: if the gap is also > this fraction of
# declared_purchases the flag is raised (whichever threshold fires first).
DEFAULT_MISMATCH_PCT_THRESHOLD: float = 0.10   # 10 %

# Window in days for the circular-trading look-back (credit → debit pairing).
DEFAULT_CIRCULAR_WINDOW_DAYS: int = 3

# Minimum amount (₹) for a transaction to be considered in circular-trading
# detection.  Only amounts that are a multiple of 1,00,000 are checked.
DEFAULT_CIRCULAR_MIN_AMOUNT: float = 1_00_000.0

# Two amounts are considered "similar" for circular-trading purposes if they
# differ by less than this fraction of the larger amount.
DEFAULT_CIRCULAR_SIMILARITY_PCT: float = 0.02   # 2 %

# Score deductions per flagged month
SCORE_DEDUCTION_INFLATION: float = 10.0
SCORE_DEDUCTION_MISMATCH: float = 7.0
SCORE_DEDUCTION_CIRCULAR: float = 15.0

# Narration keywords that indicate internal / reversal / contra credits which
# should NOT count as revenue credits.
_NON_REVENUE_NARRATION_KEYWORDS: tuple[str, ...] = (
    "reversal",
    "reverse",
    "contra",
    "own account",
    "self transfer",
    "refund",
    "return",
    "sweep",
    "fd transfer",
    "loan disbursement",
    "od limit",
)


# ---------------------------------------------------------------------------
# Configuration model
# ---------------------------------------------------------------------------

class ReconConfig(BaseModel):
    """
    All tunable thresholds in one place.  Pass a customised instance to
    ``run_gst_bank_recon`` to override defaults without touching constants.
    """
    inflation_threshold: float = Field(
        DEFAULT_INFLATION_THRESHOLD,
        ge=0.0, le=1.0,
        description="Fraction: declared_sales / bank_credits - 1 before INFLATION flag",
    )
    mismatch_abs_threshold: float = Field(
        DEFAULT_MISMATCH_ABS_THRESHOLD,
        ge=0.0,
        description="Absolute ₹ gap between 3B and 2A purchases before MISMATCH flag",
    )
    mismatch_pct_threshold: float = Field(
        DEFAULT_MISMATCH_PCT_THRESHOLD,
        ge=0.0, le=1.0,
        description="Fractional gap between 3B and 2A purchases before MISMATCH flag",
    )
    circular_window_days: int = Field(
        DEFAULT_CIRCULAR_WINDOW_DAYS,
        ge=1, le=30,
        description="Days within which a debit must follow a credit to be suspicious",
    )
    circular_min_amount: float = Field(
        DEFAULT_CIRCULAR_MIN_AMOUNT,
        ge=0.0,
        description="Minimum transaction amount (₹) considered for circular trading",
    )
    circular_similarity_pct: float = Field(
        DEFAULT_CIRCULAR_SIMILARITY_PCT,
        ge=0.0, le=1.0,
        description="Two amounts are 'similar' if they differ by less than this fraction",
    )


# ---------------------------------------------------------------------------
# Input models
# ---------------------------------------------------------------------------

class GSTR3BMonth(BaseModel):
    """
    A single month's GSTR-3B filing (self-declared).

    ``month`` must be the first day of the calendar month (e.g. 2024-01-01).
    """
    month: date
    declared_sales: float = Field(..., ge=0.0)
    declared_purchases: float = Field(..., ge=0.0)

    @field_validator("month")
    @classmethod
    def month_must_be_first_of_month(cls, v: date) -> date:
        if v.day != 1:
            raise ValueError(
                f"month must be the first day of the month, got {v}. "
                f"Use {v.replace(day=1)} instead."
            )
        return v


class GSTR2AMonth(BaseModel):
    """
    Auto-populated GSTR-2A data for a single month.

    Reflects what the entity's suppliers actually reported to GSTN.
    """
    month: date
    supplier_reported_purchases: float = Field(..., ge=0.0)

    @field_validator("month")
    @classmethod
    def month_must_be_first_of_month(cls, v: date) -> date:
        if v.day != 1:
            raise ValueError(
                f"month must be the first day of the month, got {v}."
            )
        return v


class BankTransaction(BaseModel):
    """
    A single bank statement entry.

    ``narration`` is used for filtering out non-revenue credits (reversals,
    own-account transfers, loan disbursements, etc.).
    """
    date: date
    amount: float = Field(..., ge=0.0)
    type: Literal["credit", "debit"]
    narration: str = Field(default="", description="Transaction description / remarks")


# ---------------------------------------------------------------------------
# Output models
# ---------------------------------------------------------------------------

class MonthlyReconResult(BaseModel):
    """
    Reconciliation outcome for a single calendar month.

    Attributes
    ----------
    month:
        First day of the month this record covers.
    declared_sales:
        GSTR-3B self-declared sales for the month (₹).
    bank_revenue_credits:
        Filtered bank credits considered to be revenue receipts (₹).
    divergence_pct:
        ``(declared_sales - bank_revenue_credits) / max(declared_sales, 1)``.
        Positive → GST > bank (possible inflation).
        Negative → bank > GST (possible under-declaration).
    declared_purchases:
        GSTR-3B self-declared purchases (₹), or 0 if no 3B data.
    supplier_reported_purchases:
        GSTR-2A supplier-reported purchases (₹), or 0 if no 2A data.
    issues:
        List of flag strings: one or more of
        ``REVENUE_INFLATION``, ``GSTR_2A_3B_MISMATCH``,
        ``CIRCULAR_TRADING_SUSPECTED``.
    summary:
        Human-readable single-sentence description of the month's status.
    """
    month: date
    declared_sales: float
    bank_revenue_credits: float
    divergence_pct: float
    declared_purchases: float = 0.0
    supplier_reported_purchases: float = 0.0
    issues: list[str] = Field(default_factory=list)
    summary: str = ""


class ReconResult(BaseModel):
    """
    Full reconciliation output for a company across all available months.

    Attributes
    ----------
    company_id:
        Identifier of the company / lending case.
    revenue_reliability_score:
        0–100 composite score.  100 = no anomalies detected; lower values
        reflect the cumulative number and severity of flags raised.
    months:
        Per-month detail, sorted ascending by month.
    flagged_month_count:
        Number of months in which at least one issue was detected.
    total_months_analysed:
        Total months for which at least 3B or bank data was available.
    """
    company_id: str
    revenue_reliability_score: float = Field(ge=0.0, le=100.0)
    months: list[MonthlyReconResult]
    flagged_month_count: int = 0
    total_months_analysed: int = 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _first_of_month(d: date) -> date:
    """Return the first day of the month containing *d*."""
    return d.replace(day=1)


def _is_revenue_credit(txn: BankTransaction) -> bool:
    """
    Return True when a transaction should count as a revenue receipt.

    Rules (all must hold):
    1. type == "credit"
    2. amount > 0
    3. narration does NOT contain any non-revenue keyword (case-insensitive).
    """
    if txn.type != "credit" or txn.amount <= 0:
        return False
    narration_lower = txn.narration.lower()
    return not any(kw in narration_lower for kw in _NON_REVENUE_NARRATION_KEYWORDS)


def _aggregate_bank_credits_by_month(
    txns: list[BankTransaction],
) -> dict[date, float]:
    """
    Return a ``{first_of_month: total_revenue_credits}`` dict.

    Only transactions passing ``_is_revenue_credit`` are included.
    """
    totals: dict[date, float] = defaultdict(float)
    for txn in txns:
        if _is_revenue_credit(txn):
            totals[_first_of_month(txn.date)] += txn.amount
    return dict(totals)


def _detect_circular_trading(
    txns: list[BankTransaction],
    month: date,
    config: ReconConfig,
) -> bool:
    """
    Return True if at least one circular-trading pattern is detected in
    *month*.

    Algorithm
    ---------
    1. Collect all credits and debits in the month that are:
       - ≥ ``config.circular_min_amount``
       - A multiple of 1,00,000 (round-number heuristic)
    2. For each credit, look for a debit within ``config.circular_window_days``
       whose amount differs by less than ``config.circular_similarity_pct``
       of the larger of the two amounts.
    3. If any such (credit, debit) pair is found, return True.

    The round-number filter substantially reduces false positives — routine
    salary or vendor payments are rarely exact lakhs.
    """
    month_end = (month.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    # Filter transactions to this month
    month_txns = [
        t for t in txns
        if month <= t.date <= month_end
        and t.amount >= config.circular_min_amount
        and _is_round_lakh(t.amount)
    ]

    credits = [t for t in month_txns if t.type == "credit"]
    debits  = [t for t in month_txns if t.type == "debit"]

    for credit, debit in itertools.product(credits, debits):
        # Debit must follow the credit within the rolling window
        if not (0 <= (debit.date - credit.date).days <= config.circular_window_days):
            continue
        # Amounts must be similar within the configured tolerance
        larger = max(credit.amount, debit.amount)
        if abs(credit.amount - debit.amount) / larger <= config.circular_similarity_pct:
            return True

    return False


def _is_round_lakh(amount: float) -> bool:
    """Return True if *amount* is a multiple of 1,00,000 (within ₹1 tolerance)."""
    return (amount % 1_00_000) < 1.0 or (1_00_000 - amount % 1_00_000) < 1.0


def _build_summary(issues: list[str], divergence_pct: float) -> str:
    """Produce a one-sentence plain-English summary for the month."""
    if not issues:
        return "No anomalies detected; month appears clean."

    parts: list[str] = []
    if "REVENUE_INFLATION" in issues:
        pct_str = f"{divergence_pct * 100:.1f}%"
        parts.append(f"declared GST sales exceed bank credits by {pct_str}")
    if "GSTR_2A_3B_MISMATCH" in issues:
        parts.append("3B purchase claims exceed supplier-reported 2A figures")
    if "CIRCULAR_TRADING_SUSPECTED" in issues:
        parts.append("round-number credits followed by near-identical debits within 3 days")

    return "Issues detected: " + "; ".join(parts) + "."


def _compute_score(months: list[MonthlyReconResult]) -> tuple[float, int]:
    """
    Compute the revenue reliability score and return (score, flagged_count).

    Scoring logic
    -------------
    Start at 100.

    For each month that has at least one issue:
      - REVENUE_INFLATION:          subtract SCORE_DEDUCTION_INFLATION  (10 pts)
      - GSTR_2A_3B_MISMATCH:        subtract SCORE_DEDUCTION_MISMATCH   ( 7 pts)
      - CIRCULAR_TRADING_SUSPECTED: subtract SCORE_DEDUCTION_CIRCULAR   (15 pts)

    A single month can contribute multiple deductions if it triggers multiple
    flags.  The score is floored at 0.

    Rationale for weights
    ---------------------
    - Circular trading is weighted most heavily (15 pts) because it is a
      deliberate structuring behaviour with no innocent explanation.
    - Revenue inflation (10 pts) is significant but may have legitimate
      causes such as timing differences in payment receipt.
    - 2A/3B mismatch (7 pts) is the least severe as it may reflect supplier
      filing delays rather than fraud.
    """
    score = 100.0
    flagged = 0

    for month in months:
        if not month.issues:
            continue
        flagged += 1
        if "REVENUE_INFLATION" in month.issues:
            score -= SCORE_DEDUCTION_INFLATION
        if "GSTR_2A_3B_MISMATCH" in month.issues:
            score -= SCORE_DEDUCTION_MISMATCH
        if "CIRCULAR_TRADING_SUSPECTED" in month.issues:
            score -= SCORE_DEDUCTION_CIRCULAR

    return max(score, 0.0), flagged


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_gst_bank_recon(
    company_id: str,
    gstr3b: list[GSTR3BMonth],
    gstr2a: list[GSTR2AMonth],
    txns: list[BankTransaction],
    config: ReconConfig | None = None,
) -> ReconResult:
    """
    Run the full GST-vs-bank reconciliation for a company.

    Parameters
    ----------
    company_id:
        Unique identifier for the company / lending case.
    gstr3b:
        Monthly GSTR-3B filings (self-declared sales and purchases).
    gstr2a:
        Monthly GSTR-2A data (supplier-reported purchases from GSTN).
    txns:
        Raw bank statement transactions.  Credits are filtered to revenue
        receipts; both credits and debits are used for circular-trading
        detection.
    config:
        Optional :class:`ReconConfig` to override detection thresholds.
        Defaults to ``ReconConfig()`` (all library defaults).

    Returns
    -------
    ReconResult
        Full reconciliation output with per-month detail and an overall
        revenue reliability score (0–100).

    Notes
    -----
    - Months present in bank data but absent from GSTR-3B are still analysed
      (declared_sales defaults to 0).
    - Months present in GSTR-3B but absent from bank data are also analysed
      (bank_revenue_credits defaults to 0).
    - GSTR-2A months without a matching GSTR-3B row are skipped (no
      declared_purchases to compare against).
    """
    if config is None:
        config = ReconConfig()

    # ------------------------------------------------------------------
    # Step 1: Build lookup dicts keyed by first-of-month date
    # ------------------------------------------------------------------
    gstr3b_by_month: dict[date, GSTR3BMonth] = {
        r.month: r for r in gstr3b
    }
    gstr2a_by_month: dict[date, GSTR2AMonth] = {
        r.month: r for r in gstr2a
    }
    bank_credits_by_month: dict[date, float] = _aggregate_bank_credits_by_month(txns)

    # ------------------------------------------------------------------
    # Step 2: Determine the universe of months to analyse
    # Union of all months appearing in any data source.
    # ------------------------------------------------------------------
    all_months: set[date] = (
        set(gstr3b_by_month.keys())
        | set(bank_credits_by_month.keys())
    )

    # ------------------------------------------------------------------
    # Step 3: Process each month
    # ------------------------------------------------------------------
    monthly_results: list[MonthlyReconResult] = []

    for month in sorted(all_months):
        gst_row = gstr3b_by_month.get(month)
        gst2a_row = gstr2a_by_month.get(month)

        declared_sales = gst_row.declared_sales if gst_row else 0.0
        declared_purchases = gst_row.declared_purchases if gst_row else 0.0
        supplier_reported = gst2a_row.supplier_reported_purchases if gst2a_row else 0.0
        bank_credits = bank_credits_by_month.get(month, 0.0)

        issues: list[str] = []

        # ---------------------------------------------------------------
        # Check 1: Revenue inflation
        # divergence_pct = (declared_sales - bank_credits) / max(declared_sales, 1)
        # Using max(..., 1) prevents division by zero when declared_sales == 0.
        # A positive divergence means GST > bank (declared more than received).
        # ---------------------------------------------------------------
        divergence_pct = (declared_sales - bank_credits) / max(declared_sales, 1.0)
        if divergence_pct > config.inflation_threshold:
            issues.append("REVENUE_INFLATION")

        # ---------------------------------------------------------------
        # Check 2: GSTR-2A vs 3B purchase mismatch
        # Only run when both 3B and 2A data are available for the month.
        # The entity may be claiming more ITC than its suppliers reported.
        # ---------------------------------------------------------------
        if gst_row is not None and gst2a_row is not None:
            purchase_gap = declared_purchases - supplier_reported
            pct_gap = purchase_gap / max(declared_purchases, 1.0)
            if purchase_gap > config.mismatch_abs_threshold or pct_gap > config.mismatch_pct_threshold:
                issues.append("GSTR_2A_3B_MISMATCH")

        # ---------------------------------------------------------------
        # Check 3: Circular trading heuristic
        # Look for round-lakh credit-then-debit pairs within 3 days.
        # ---------------------------------------------------------------
        if _detect_circular_trading(txns, month, config):
            issues.append("CIRCULAR_TRADING_SUSPECTED")

        summary = _build_summary(issues, divergence_pct)

        monthly_results.append(MonthlyReconResult(
            month=month,
            declared_sales=declared_sales,
            bank_revenue_credits=bank_credits,
            divergence_pct=round(divergence_pct, 6),
            declared_purchases=declared_purchases,
            supplier_reported_purchases=supplier_reported,
            issues=issues,
            summary=summary,
        ))

    # ------------------------------------------------------------------
    # Step 4: Compute overall score
    # ------------------------------------------------------------------
    score, flagged = _compute_score(monthly_results)

    return ReconResult(
        company_id=company_id,
        revenue_reliability_score=round(score, 2),
        months=monthly_results,
        flagged_month_count=flagged,
        total_months_analysed=len(monthly_results),
    )