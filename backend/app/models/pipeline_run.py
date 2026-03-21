import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID, ARRAY

from app.db.session import Base


class PipelineRun(Base):
    __tablename__ = 'pipeline_runs'

    run_id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hotel           = Column(String(50), nullable=False, default='hickstead')
    triggered_by    = Column(String(50))
    status          = Column(String(20), nullable=False)
    steps_completed = Column(ARRAY(Text))
    steps_failed    = Column(ARRAY(Text))
    rows_written    = Column(Integer)
    error_message   = Column(Text)
    started_at      = Column(DateTime(timezone=True), default=datetime.utcnow)
    finished_at     = Column(DateTime(timezone=True))
