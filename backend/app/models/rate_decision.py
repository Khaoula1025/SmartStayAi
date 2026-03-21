import uuid
from datetime import datetime

from sqlalchemy import Column, Date, DateTime, ForeignKey, Numeric, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.session import Base


class RateDecision(Base):
    __tablename__ = 'rate_decisions'

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hotel            = Column(String(50), nullable=False, default='hickstead')
    date             = Column(Date,       nullable=False)
    prediction_id    = Column(UUID(as_uuid=True), ForeignKey('predictions.id'),
                              nullable=True)
    recommended_rate = Column(Numeric(7,2), nullable=False)
    final_rate       = Column(Numeric(7,2))
    action           = Column(String(10),   nullable=False)  # accept|override|ignore
    override_reason  = Column(Text)
    user_id          = Column(String(100))
    decided_at       = Column(DateTime(timezone=True), default=datetime.utcnow)

    prediction       = relationship('Prediction', back_populates='rate_decisions')
