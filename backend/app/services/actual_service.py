from datetime import date
from pathlib import Path
from typing import Optional, List

import pandas as pd
from sqlalchemy.orm import Session

from app.models.actual import Actual

HOTEL = 'hickstead'


def get_actuals(
    db:        Session,
    date_from: Optional[date] = None,
    date_to:   Optional[date] = None,
    limit:     int            = 500,
) -> List[Actual]:
    q = db.query(Actual).filter(Actual.hotel == HOTEL)
    if date_from: q = q.filter(Actual.date >= date_from)
    if date_to:   q = q.filter(Actual.date <= date_to)
    return q.order_by(Actual.date.desc()).limit(limit).all()


def load_actuals_from_csv(db: Session, csv_path: Path) -> int:
    """
    Load clean_occupancy.csv into the actuals table.
    Called from the /actuals/load endpoint (admin only).
    Safe to call multiple times — uses upsert on (hotel, date).
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df    = pd.read_csv(csv_path, parse_dates=['date'])
    count = 0

    for _, row in df.iterrows():
        sf = lambda c: float(row[c]) if c in row.index and pd.notna(row[c]) else None
        si = lambda c: int(row[c])   if c in row.index and pd.notna(row[c]) else None

        existing = (
            db.query(Actual)
            .filter(Actual.hotel == HOTEL, Actual.date == row['date'].date())
            .first()
        )

        if existing:
            existing.occ_rate   = sf('occ_rate')
            existing.rooms_let  = si('rooms_let')
            existing.tot_rooms  = si('tot_rooms')
            existing.occ_pct    = sf('occ_pct')
        else:
            db.add(Actual(
                hotel           = HOTEL,
                date            = row['date'].date(),
                occ_rate        = sf('occ_rate'),
                rooms_let       = si('rooms_let'),
                tot_rooms       = si('tot_rooms'),
                avl             = si('avl'),
                occ_pct         = sf('occ_pct'),
                db_let          = si('DB_let'),
                db_occ_rate     = sf('DB_occ_rate'),
                db_sb_let       = si('DB_SB_let'),
                db_sb_occ_rate  = sf('DB_SB_occ_rate'),
                exec_let        = si('EXEC_let'),
                exec_occ_rate   = sf('EXEC_occ_rate'),
                tb_let          = si('TB_let'),
                tb_occ_rate     = sf('TB_occ_rate'),
                is_interpolated = bool(row.get('is_interpolated', False)),
            ))
        count += 1

    db.commit()
    return count
