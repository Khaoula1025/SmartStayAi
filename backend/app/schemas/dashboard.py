from datetime import date
from typing import Optional, List
from pydantic import BaseModel


class OccupancyKpi(BaseModel):
    """Next 30 / 60 / 90 day average predicted occupancy."""
    next_30d_avg: float
    next_60d_avg: float
    next_90d_avg: float


class PaceKpi(BaseModel):
    """How current BOB compares to budget and STLY."""
    avg_pace_gap:      float   # avg rooms ahead/behind budget
    dates_ahead:       int     # number of dates running ahead of budget
    dates_behind:      int     # number of dates running behind budget


class ModelKpi(BaseModel):
    mae_operational:  Optional[float] = None
    occ_accuracy_pct: Optional[float] = None
    model_type:       Optional[str]   = None
    last_trained:     Optional[str]   = None
    last_rescore:     Optional[str]   = None


class UpcomingHighlight(BaseModel):
    """A date worth flagging — high occ, event, or low quality data."""
    date:             date
    predicted_occ:    float
    recommended_rate: float
    rate_tier:        str
    flag:             str   # 'high_demand' | 'event' | 'low_quality' | 'bank_holiday'


class DashboardSummary(BaseModel):
    hotel:             str
    as_of:             str
    bob_quality:       str    # high | medium | low — from latest rescore
    occupancy:         OccupancyKpi
    pace:              PaceKpi
    model:             ModelKpi
    highlights:        List[UpcomingHighlight]
    alerts:            List[str]   # any warnings to show as banners
