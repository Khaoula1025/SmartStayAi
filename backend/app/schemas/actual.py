from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel


class ActualOut(BaseModel):
    date:           date
    occ_rate:       Optional[float] = None
    rooms_let:      Optional[int]   = None
    tot_rooms:      Optional[int]   = None
    occ_pct:        Optional[float] = None
    adr:            Optional[float] = None
    revpar:         Optional[float] = None
    is_interpolated: Optional[bool] = None

    class Config:
        from_attributes = True
