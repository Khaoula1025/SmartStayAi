import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey,
    Integer, Numeric, SmallInteger, String, Text,
)
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship

from app.db.session import Base


class ModelRun(Base):
    __tablename__ = 'model_runs'

    run_id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hotel             = Column(String(50),  nullable=False, default='hickstead')
    trained_at        = Column(DateTime(timezone=True), default=datetime.utcnow)
    n_training_rows   = Column(Integer,     nullable=False)
    n_prediction_rows = Column(Integer,     nullable=False)
    mae_operational   = Column(Numeric(6,4))
    mae_all_folds     = Column(Numeric(6,4))
    r2_mean           = Column(Numeric(6,4))
    occ_accuracy_pct  = Column(Numeric(5,2))
    features          = Column(ARRAY(Text))
    model_type        = Column(String(100))
    stage2_regime     = Column(String(20))
    promoted          = Column(Boolean, nullable=False, default=False)
    notes             = Column(Text)
    created_at        = Column(DateTime(timezone=True), default=datetime.utcnow)

    predictions       = relationship('Prediction', back_populates='model_run',
                                     cascade='all, delete-orphan')


class Prediction(Base):
    __tablename__ = 'predictions'

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hotel            = Column(String(50),   nullable=False, default='hickstead')
    run_id           = Column(UUID(as_uuid=True), ForeignKey('model_runs.run_id'),
                              nullable=False)
    date             = Column(Date,         nullable=False)
    day_of_week      = Column(String(10))
    month            = Column(SmallInteger)
    days_ahead       = Column(SmallInteger)
    stage1_occ       = Column(Numeric(5,4))
    pace_adj         = Column(Numeric(6,4))
    bob_adj          = Column(Numeric(6,4))
    predicted_occ    = Column(Numeric(5,4), nullable=False)
    predicted_rooms  = Column(SmallInteger, nullable=False)
    occ_low          = Column(Numeric(5,4))
    occ_high         = Column(Numeric(5,4))
    recommended_rate = Column(Numeric(7,2), nullable=False)
    rate_tier        = Column(String(15))
    floor_price      = Column(Numeric(7,2))
    bob_occ          = Column(Numeric(5,4))
    pace_gap         = Column(SmallInteger)
    pickup_velocity  = Column(Numeric(7,4))
    is_bank_holiday  = Column(SmallInteger, default=0)
    is_local_event   = Column(SmallInteger, default=0)
    data_quality     = Column(String(10))
    created_at       = Column(DateTime(timezone=True), default=datetime.utcnow)

    model_run        = relationship('ModelRun', back_populates='predictions')
    rate_decisions   = relationship('RateDecision', back_populates='prediction')
