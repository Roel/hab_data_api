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

        # 1. Resample and expand to the new frequency
        # .asfreq() creates the new rows with NaN values
        resampled = df.resample(freq).asfreq()

        # 2. Interpolate the numeric 'value' column
        resampled["value"] = resampled["value"].interpolate(
            method=self.interpolation_method
        )

        # 3. Forward-fill the 'unit' column (strings)
        resampled["unit"] = resampled["unit"].ffill()

        # 4. Final ffill to catch any leading NaNs if necessary
        return resampled.ffill()
