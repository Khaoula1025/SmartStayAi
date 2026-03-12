import uuid
from datetime import datetime

from sqlalchemy import Boolean, Column, Date, DateTime, Numeric, SmallInteger, String
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class Actual(Base):
    __tablename__ = 'actuals'

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hotel           = Column(String(50), nullable=False, default='hickstead')
    date            = Column(Date,       nullable=False, unique=True)
    occ_rate        = Column(Numeric(5,4))
    rooms_let       = Column(SmallInteger)
    tot_rooms       = Column(SmallInteger)
    avl             = Column(SmallInteger)
    occ_pct         = Column(Numeric(5,4))
    adr             = Column(Numeric(7,2))
    revpar          = Column(Numeric(7,2))
    db_let          = Column(SmallInteger)
    db_occ_rate     = Column(Numeric(5,4))
    db_sb_let       = Column(SmallInteger)
    db_sb_occ_rate  = Column(Numeric(5,4))
    exec_let        = Column(SmallInteger)
    exec_occ_rate   = Column(Numeric(5,4))
    tb_let          = Column(SmallInteger)
    tb_occ_rate     = Column(Numeric(5,4))
    is_interpolated = Column(Boolean, default=False)
    loaded_at       = Column(DateTime(timezone=True), default=datetime.utcnow)
