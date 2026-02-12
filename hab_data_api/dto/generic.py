from dataclasses import dataclass
from typing import Optional

import datetime
import pandas as pd


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
    sum: Optional[float] = None


@dataclass
class TimeDataInterpolatedRangeDto:
    interpolation_method: str
    data: list

    def to_df(self, freq):
        df = pd.DataFrame(columns=("timestamp", "value", "unit"), data=self.data)
        df = df.set_index("timestamp")
        df = df.resample(freq).interpolate(self.interpolation_method).ffill()
        return df
