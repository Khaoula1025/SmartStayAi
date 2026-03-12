from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field


class RateDecisionIn(BaseModel):
    date:             date
    recommended_rate: float
    final_rate:       Optional[float] = None
    action:           str = Field(..., pattern='^(accept|override|ignore)$')
    override_reason:  Optional[str]   = None


class RateDecisionOut(BaseModel):
    id:               str
    date:             date
    recommended_rate: float
    final_rate:       Optional[float] = None
    action:           str
    override_reason:  Optional[str]   = None
    user_id:          Optional[str]   = None
    decided_at:       datetime
