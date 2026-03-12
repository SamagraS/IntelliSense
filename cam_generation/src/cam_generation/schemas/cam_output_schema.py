from pydantic import BaseModel
from typing import List, Optional


class CAMOutput(BaseModel):
    docx: str
    pdf: Optional[str] = None
    sections: List[str]