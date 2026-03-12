from pydantic import BaseModel
from typing import Optional


class LoanDetails(BaseModel):
    loan_type: str
    proposed_amount_inr: float
    tenure_months: int
    proposed_interest_rate_pct: Optional[float] = None
    loan_purpose: Optional[str] = None


class CaseMetadata(BaseModel):
    case_id: str
    company_name: str
    sector: str
    sub_sector: Optional[str] = None
    state: Optional[str] = None
    annual_turnover_cr: Optional[float] = None
    loan_details: Optional[LoanDetails] = None


class CAMPayload(BaseModel):
    case_metadata: CaseMetadata