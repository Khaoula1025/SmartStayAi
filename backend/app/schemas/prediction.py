from datetime import date
from typing import Optional
from uuid import UUID
from pydantic import BaseModel


class PredictionOut(BaseModel):
    date:             date
    day_of_week:      Optional[str]   = None
    month:            Optional[int]   = None
    days_ahead:       Optional[int]   = None
    predicted_occ:    float
    predicted_rooms:  int
    occ_low:          Optional[float] = None
    occ_high:         Optional[float] = None
    recommended_rate: float
    rate_tier:        Optional[str]   = None
    floor_price:      Optional[float] = None
    bob_occ:          Optional[float] = None
    pace_gap:         Optional[int]   = None
    stage1_occ:       Optional[float] = None
    pace_adj:         Optional[float] = None
    bob_adj:          Optional[float] = None
    is_bank_holiday:  Optional[int]   = None
    is_local_event:   Optional[int]   = None
    data_quality:     Optional[str]   = None

    class Config:
        from_attributes = True


class ModelRunOut(BaseModel):
    run_id:            UUID
    trained_at:        str
    model_type:        Optional[str]   = None
    n_training_rows:   int
    n_prediction_rows: int
    mae_operational:   Optional[float] = None
    occ_accuracy_pct:  Optional[float] = None
    promoted:          bool

    class Config:
        from_attributes = True
