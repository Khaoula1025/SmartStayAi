from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class PipelineStatusOut(BaseModel):
    run_id:          str
    status:          str
    triggered_by:    Optional[str]       = None
    steps_completed: Optional[List[str]] = None
    steps_failed:    Optional[List[str]] = None
    rows_written:    Optional[int]       = None
    error_message:   Optional[str]       = None
    started_at:      datetime
    finished_at:     Optional[datetime]  = None

    class Config:
        from_attributes = True
