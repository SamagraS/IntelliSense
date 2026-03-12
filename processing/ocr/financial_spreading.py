"""
financial_spreading.py
======================
Computes key financial ratios from normalised yearly financial statements.

Ratios computed per year
------------------------
  DSCR              = net_operating_income / total_debt_service
  Leverage          = total_debt / net_worth
  Interest Coverage = ebit / interest_expense
  Current Ratio     = current_assets / current_liabilities
  EBITDA Margin     = ebitda / revenue
  PAT Margin        = pat / revenue

Ratios computed across 3 years
-------------------------------
  Revenue CAGR = (revenue_N / revenue_N-2) ^ (1/2) - 1

Usage
-----
    from financial_spreading import compute_ratios, FinancialYear

    years = [
        FinancialYear(year=2022, revenue=1000, ...),
        FinancialYear(year=2023, revenue=1150, ...),
        FinancialYear(year=2024, revenue=1300, ...),
    ]
    result = compute_ratios("COMP_001", years)
"""

from __future__ import annotations

import math
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Sentinel used when a ratio cannot be computed (zero denominator, etc.)
# ---------------------------------------------------------------------------
_NOT_COMPUTED = None


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class RatioKey(str, Enum):
    DSCR = "dscr"
    LEVERAGE = "leverage"
    INTEREST_COVERAGE = "interest_coverage"
    CURRENT_RATIO = "current_ratio"
    EBITDA_MARGIN = "ebitda_margin"
    PAT_MARGIN = "pat_margin"


class ComputeStatus(str, Enum):
    OK = "ok"
    ZERO_DENOMINATOR = "zero_denominator"
    NEGATIVE_BASE = "negative_base"          # CAGR with negative/zero start revenue
    INSUFFICIENT_DATA = "insufficient_data"


# ---------------------------------------------------------------------------
# Input model
# ---------------------------------------------------------------------------

class FinancialYear(BaseModel):
    """
    Normalised financial statement data for a single fiscal year.

    All monetary values are in the same currency unit (e.g. ₹ Crore).
    No unit conversion is performed here — the caller is responsible for
    ensuring consistency across years.
    """
    year: int = Field(..., description="Fiscal year (e.g. 2024)")
    revenue: float = Field(..., description="Total revenue / net sales")
    ebitda: float = Field(..., description="Earnings before interest, tax, depreciation & amortisation")
    ebit: float = Field(..., description="Earnings before interest and tax")
    pat: float = Field(..., description="Profit after tax")
    net_operating_income: float = Field(..., description="Net operating income used for DSCR")
    total_debt_service: float = Field(..., description="Total debt service (principal repayment + interest)")
    total_debt: float = Field(..., description="Total outstanding debt (short-term + long-term)")
    net_worth: float = Field(..., description="Shareholders' equity / net worth")
    current_assets: float = Field(..., description="Current assets (within 12 months)")
    current_liabilities: float = Field(..., description="Current liabilities (within 12 months)")
    interest_expense: float = Field(..., description="Interest expense / finance cost")

    @model_validator(mode="after")
    def year_must_be_plausible(self) -> "FinancialYear":
        if not (1900 <= self.year <= 2100):
            raise ValueError(f"year {self.year} is outside the plausible range 1900–2100")
        return self


# ---------------------------------------------------------------------------
# Output models
# ---------------------------------------------------------------------------

class RatioDetail(BaseModel):
    """
    A single computed ratio with full provenance.

    Attributes
    ----------
    value:
        The computed ratio, or ``None`` when the ratio cannot be calculated
        (e.g. zero denominator).  Always ``None`` when ``status != ok``.
    inputs:
        The raw input values used in the formula, keyed by variable name.
        Included even when ``value`` is ``None`` so the UI can show what
        inputs were available.
    formula:
        Human-readable formula string (e.g. ``"ebit / interest_expense"``).
    status:
        ``ok``               — value was computed successfully.
        ``zero_denominator`` — denominator was zero; value is None.
        ``negative_base``    — CAGR start revenue ≤ 0; value is None.
        ``insufficient_data``— not enough years provided; value is None.
    note:
        Optional plain-text explanation for non-ok statuses.
    """
    value: Optional[float] = None
    inputs: dict[str, Any] = Field(default_factory=dict)
    formula: str
    status: ComputeStatus = ComputeStatus.OK
    note: Optional[str] = None

    model_config = {"use_enum_values": True}


class YearlyRatios(BaseModel):
    """All per-year ratios for a single fiscal year."""
    year: int
    ratios: dict[str, RatioDetail]


class RatiosResult(BaseModel):
    """
    Complete spreading output for a company.

    Attributes
    ----------
    company_id:
        Identifier of the company / lending case.
    yearly:
        Per-year ratios, sorted ascending by year.
    revenue_cagr:
        3-year compounded annual growth rate of revenue (oldest to newest year).
    years_used:
        The fiscal years that were used in this computation, ascending.
    """
    company_id: str
    yearly: list[YearlyRatios]
    revenue_cagr: RatioDetail
    years_used: list[int]


# ---------------------------------------------------------------------------
# Internal ratio computation helpers
# ---------------------------------------------------------------------------

def _safe_divide(
    numerator: float,
    denominator: float,
    formula: str,
    inputs: dict[str, Any],
) -> RatioDetail:
    """
    Divide *numerator* by *denominator*, returning a :class:`RatioDetail`.

    If ``denominator == 0``, returns a ``RatioDetail`` with ``value=None``
    and ``status=zero_denominator`` so the caller always gets a well-formed
    object regardless of input quality.
    """
    if denominator == 0.0:
        return RatioDetail(
            value=None,
            inputs=inputs,
            formula=formula,
            status=ComputeStatus.ZERO_DENOMINATOR,
            note=(
                f"Cannot compute: denominator is zero "
                f"(formula: {formula}). "
                "Check whether this line item is applicable for this entity."
            ),
        )

    raw = numerator / denominator
    # Round to 6 significant decimal places — avoids floating-point noise
    # while preserving enough precision for financial analysis.
    value = round(raw, 6)
    return RatioDetail(
        value=value,
        inputs=inputs,
        formula=formula,
        status=ComputeStatus.OK,
    )


def _compute_dscr(fy: FinancialYear) -> RatioDetail:
    """
    Debt Service Coverage Ratio.
    Measures how many times operating income covers debt obligations.
    A value < 1.0 signals the entity cannot service its debt from operations.

    Formula: net_operating_income / total_debt_service
    """
    return _safe_divide(
        numerator=fy.net_operating_income,
        denominator=fy.total_debt_service,
        formula="net_operating_income / total_debt_service",
        inputs={
            "net_operating_income": fy.net_operating_income,
            "total_debt_service": fy.total_debt_service,
        },
    )


def _compute_leverage(fy: FinancialYear) -> RatioDetail:
    """
    Leverage Ratio (Debt-to-Equity).
    Higher values indicate greater financial risk.

    Formula: total_debt / net_worth
    """
    return _safe_divide(
        numerator=fy.total_debt,
        denominator=fy.net_worth,
        formula="total_debt / net_worth",
        inputs={
            "total_debt": fy.total_debt,
            "net_worth": fy.net_worth,
        },
    )


def _compute_interest_coverage(fy: FinancialYear) -> RatioDetail:
    """
    Interest Coverage Ratio (Times Interest Earned).
    Measures how comfortably EBIT covers interest obligations.
    Values < 1.5x are typically flagged as high risk.

    Formula: ebit / interest_expense
    """
    return _safe_divide(
        numerator=fy.ebit,
        denominator=fy.interest_expense,
        formula="ebit / interest_expense",
        inputs={
            "ebit": fy.ebit,
            "interest_expense": fy.interest_expense,
        },
    )


def _compute_current_ratio(fy: FinancialYear) -> RatioDetail:
    """
    Current Ratio.
    A ratio < 1.0 means the entity cannot cover short-term obligations
    with short-term assets (liquidity risk).

    Formula: current_assets / current_liabilities
    """
    return _safe_divide(
        numerator=fy.current_assets,
        denominator=fy.current_liabilities,
        formula="current_assets / current_liabilities",
        inputs={
            "current_assets": fy.current_assets,
            "current_liabilities": fy.current_liabilities,
        },
    )


def _compute_ebitda_margin(fy: FinancialYear) -> RatioDetail:
    """
    EBITDA Margin.
    Expressed as a decimal (e.g. 0.18 = 18%).
    Measures operational efficiency before capital structure effects.

    Formula: ebitda / revenue
    """
    return _safe_divide(
        numerator=fy.ebitda,
        denominator=fy.revenue,
        formula="ebitda / revenue",
        inputs={
            "ebitda": fy.ebitda,
            "revenue": fy.revenue,
        },
    )


def _compute_pat_margin(fy: FinancialYear) -> RatioDetail:
    """
    PAT (Net Profit) Margin.
    Expressed as a decimal. The bottom-line profitability metric.

    Formula: pat / revenue
    """
    return _safe_divide(
        numerator=fy.pat,
        denominator=fy.revenue,
        formula="pat / revenue",
        inputs={
            "pat": fy.pat,
            "revenue": fy.revenue,
        },
    )


def _compute_revenue_cagr(
    oldest: FinancialYear,
    newest: FinancialYear,
) -> RatioDetail:
    """
    3-Year Revenue Compound Annual Growth Rate.

    Uses the two boundary years of the 3-year window.  The number of
    compounding periods is always 2 (oldest → middle → newest).

    Formula: (revenue_N / revenue_N-2) ^ (1/2) - 1

    Edge cases
    ----------
    - ``revenue_N-2 <= 0``  → ``negative_base`` status (CAGR undefined)
    - ``revenue_N < 0``     → value is computed but may look unusual;
                               a negative CAGR with a loss-making end year
                               is mathematically valid.
    - ``revenue_N-2 == 0``  → ``zero_denominator`` status
    """
    n_periods = newest.year - oldest.year   # typically 2 for a 3-year window
    if n_periods <= 0:
        # Caller passed years in wrong order or identical years — guard
        n_periods = 2

    inputs = {
        "revenue_oldest": oldest.revenue,
        "revenue_newest": newest.revenue,
        "year_oldest": oldest.year,
        "year_newest": newest.year,
        "n_periods": n_periods,
    }
    formula = f"(revenue_{newest.year} / revenue_{oldest.year}) ^ (1/{n_periods}) - 1"

    if oldest.revenue == 0.0:
        return RatioDetail(
            value=None,
            inputs=inputs,
            formula=formula,
            status=ComputeStatus.ZERO_DENOMINATOR,
            note=(
                f"Cannot compute CAGR: base year ({oldest.year}) revenue is zero. "
                "Check whether the entity had no operations in that year."
            ),
        )

    if oldest.revenue < 0:
        return RatioDetail(
            value=None,
            inputs=inputs,
            formula=formula,
            status=ComputeStatus.NEGATIVE_BASE,
            note=(
                f"Cannot compute CAGR: base year ({oldest.year}) revenue is negative "
                f"({oldest.revenue}). CAGR is undefined for negative base values."
            ),
        )

    ratio = newest.revenue / oldest.revenue
    # For a negative end-year revenue with positive base, ratio < 0 → math.pow
    # of a negative number with a fractional exponent is undefined in reals.
    # In that case we return the signed magnitude approach used in practice.
    if ratio < 0:
        cagr = -(abs(ratio) ** (1.0 / n_periods)) - 1
    else:
        cagr = (ratio ** (1.0 / n_periods)) - 1

    return RatioDetail(
        value=round(cagr, 6),
        inputs=inputs,
        formula=formula,
        status=ComputeStatus.OK,
    )


def _compute_yearly_ratios(fy: FinancialYear) -> YearlyRatios:
    """Compute all per-year ratios for a single :class:`FinancialYear`."""
    return YearlyRatios(
        year=fy.year,
        ratios={
            RatioKey.DSCR:              _compute_dscr(fy),
            RatioKey.LEVERAGE:          _compute_leverage(fy),
            RatioKey.INTEREST_COVERAGE: _compute_interest_coverage(fy),
            RatioKey.CURRENT_RATIO:     _compute_current_ratio(fy),
            RatioKey.EBITDA_MARGIN:     _compute_ebitda_margin(fy),
            RatioKey.PAT_MARGIN:        _compute_pat_margin(fy),
        },
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_ratios(
    company_id: str,
    years: list[FinancialYear],
) -> RatiosResult:
    """
    Compute financial ratios from a list of yearly financial statements.

    Parameters
    ----------
    company_id:
        Unique identifier for the company / lending case (stored verbatim
        in the result).
    years:
        List of :class:`FinancialYear` records.  Must contain **at least 3**
        entries; only the **3 most recent years** (sorted by ``year``) are
        used in the computation.  Duplicate years are de-duplicated by keeping
        the last occurrence.

    Returns
    -------
    RatiosResult
        Yearly per-ratio breakdowns plus the 3-year revenue CAGR.

    Raises
    ------
    ValueError
        If ``years`` is empty or contains fewer than 3 distinct years.

    Notes
    -----
    - All ratio values are rounded to 6 decimal places.
    - Ratios that cannot be computed (zero denominators, etc.) have
      ``value=None`` and a populated ``note`` field explaining why.
    - Input values in ``RatioDetail.inputs`` are the **raw** values as
      supplied — no rounding is applied to inputs, only to the result.
    """
    if not years:
        raise ValueError("years must not be empty.")

    # De-duplicate by year (last occurrence wins — matches upsert semantics)
    by_year: dict[int, FinancialYear] = {}
    for fy in years:
        by_year[fy.year] = fy

    sorted_years = sorted(by_year.values(), key=lambda fy: fy.year)

    if len(sorted_years) < 3:
        raise ValueError(
            f"At least 3 distinct fiscal years are required to compute ratios "
            f"(including revenue CAGR). Got {len(sorted_years)} distinct year(s): "
            f"{[fy.year for fy in sorted_years]}."
        )

    # Use only the 3 most recent years
    window: list[FinancialYear] = sorted_years[-3:]

    # Per-year ratios (all 3 years)
    yearly_ratios = [_compute_yearly_ratios(fy) for fy in window]

    # 3-year Revenue CAGR: oldest year in window → newest year in window
    cagr = _compute_revenue_cagr(oldest=window[0], newest=window[-1])

    return RatiosResult(
        company_id=company_id,
        yearly=yearly_ratios,
        revenue_cagr=cagr,
        years_used=[fy.year for fy in window],
    )