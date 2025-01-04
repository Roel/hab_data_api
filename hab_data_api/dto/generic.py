from dataclasses import dataclass

import datetime


@dataclass
class TimeDataDto:
    timestamp: datetime.datetime
    value: float
    unit: str


@dataclass
class TimePeriodStatsDto:
    start: datetime.datetime
    end: datetime.datetime
    unit: str
    q25: float
    q50: float
    q75: float
    stddev: float
