# HAB data API
# Copyright (C) 2023-2024  Roel Huybrechts

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from dataclasses import dataclass
import datetime

import pandas as pd

import pytz
from clients.influx import InfluxClient


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


@dataclass
class HeatPumpStatusDto:
    operating_mode: str
    heat_source: str
    defrost_status: str


@dataclass
class HeatPumpSetpointDto:
    dhw: float
    heating: float


def to_brussels_time(datetime_utc):
    return datetime_utc.replace(tzinfo=pytz.utc).astimezone(
        pytz.timezone('Europe/Brussels'))


CACHE = {}


def cache_for(seconds):
    def cache(fn):
        def wrapper(*args, **kwargs):
            timestamp, cache = CACHE.get(fn.__name__, (None, None))

            if timestamp is not None and cache is not None \
                    and timestamp >= datetime.datetime.now() - datetime.timedelta(seconds=seconds):
                return cache

            result = fn(*args, *kwargs)
            CACHE[fn.__name__] = (datetime.datetime.now(), result)
            return result
        return wrapper
    return cache


class InfluxService:
    def __init__(self, app):
        self.app = app

        self.client = InfluxClient(
            host=self.app.config['INFLUX_HOST'],
            database=self.app.config['INFLUX_DATABASE'],
            username=self.app.config['INFLUX_USERNAME'],
            password=self.app.config['INFLUX_PASSWORD']
        )

    def get_current_power_fromgrid(self):
        current_power_net = self.get_current_power_net()

        return TimeDataDto(
            timestamp=current_power_net.timestamp,
            value=max(0, current_power_net.value),
            unit='W'
        )

    @cache_for(seconds=5)
    def get_current_power_net(self):
        rs = self.client.query(
            "select * from (SELECT value FROM p1_elec_power_fromgrid order by time desc limit 1),"
            "(SELECT value * -1 FROM p1_elec_power_togrid order by time desc limit 1) order by time desc"
        )

        results = []
        for r in rs.get_points():
            r['time'] = to_brussels_time(
                datetime.datetime.strptime(r['time'], '%Y-%m-%dT%H:%M:%SZ'))
            results.append(r)

        results = sorted(results, key=lambda x: x['time'])

        current_power = results[-1]['value'] * 1000

        return TimeDataDto(
            timestamp=results[-1]['time'],
            value=current_power,
            unit='W'
        )

    @cache_for(seconds=5)
    def get_current_production(self):
        now = datetime.datetime.now().astimezone(
            pytz.utc) - datetime.timedelta(minutes=1)

        rs = self.client.query(
            f"select * from active_power where time >= '{now.isoformat()}' order by time desc limit 1"
        )

        results = []
        for r in rs.get_points():
            r['time'] = to_brussels_time(
                datetime.datetime.strptime(r['time'], '%Y-%m-%dT%H:%M:%SZ'))
            results.append(r)

        results = sorted(results, key=lambda x: x['time'])

        if len(results) > 0:
            current_production = results[-1]['value']
        else:
            current_production = 0

        return TimeDataDto(
            timestamp=results[-1]['time'],
            value=current_production,
            unit='W'
        )

    def get_current_consumption(self):
        current_production = self.get_current_production()
        current_power_net = self.get_current_power_net()

        current_consumption = current_production.value + current_power_net.value

        return TimeDataDto(
            timestamp=max(current_production.timestamp,
                          current_power_net.timestamp),
            value=current_consumption,
            unit='W'
        )

    @cache_for(seconds=600)
    def get_baseline_consumption(self):
        period = datetime.timedelta(hours=24)

        end = datetime.datetime.now().astimezone(pytz.utc)
        start = end - period

        rs_fromgrid = self.client.query(
            f"""
        SELECT difference(last(value)) as fromgrid from p1_elec_total_fromgrid
        where time > '{start.isoformat()}' and time <= '{end.isoformat()}'
        group by rate, time(5m) tz('Europe/Brussels')
        """
        )

        rs_togrid = self.client.query(
            f"""
        SELECT difference(last(value)) as togrid from p1_elec_total_togrid
        where time > '{start.isoformat()}' and time <= '{end.isoformat()}'
        group by rate, time(5m) tz('Europe/Brussels')
        """
        )

        rs_production = self.client.query(
            f"""
        SELECT difference(last(value)) as production from accumulated_yield_energy
        where time > '{start.isoformat()}' and time <= '{end.isoformat()}'
        group by time(5m) tz('Europe/Brussels')
        """
        )

        # fromgrid
        df_rate1 = pd.DataFrame(
            list(rs_fromgrid.get_points(tags={'rate': 'rate1'}))[:-1]
        )
        df_rate1 = df_rate1.set_index('time').rename(
            columns={'fromgrid': 'rate1'})

        df_rate2 = pd.DataFrame(
            list(rs_fromgrid.get_points(tags={'rate': 'rate2'}))[:-1]
        )
        df_rate2 = df_rate2.set_index('time').rename(
            columns={'fromgrid': 'rate2'})

        df = pd.merge(df_rate1, df_rate2, left_index=True, right_index=True)
        df['fromgrid'] = df.rate1 + df.rate2
        df_result = df[['fromgrid']]

        # togrid
        df_rate1 = pd.DataFrame(
            list(rs_togrid.get_points(tags={'rate': 'rate1'}))[:-1]
        )
        df_rate1 = df_rate1.set_index('time').rename(
            columns={'togrid': 'rate1'})

        df_rate2 = pd.DataFrame(
            list(rs_togrid.get_points(tags={'rate': 'rate2'}))[:-1]
        )
        df_rate2 = df_rate2.set_index('time').rename(
            columns={'togrid': 'rate2'})

        df = pd.merge(df_rate1, df_rate2, left_index=True, right_index=True)
        df['togrid'] = df.rate1 + df.rate2
        df_togrid = df[['togrid']]

        df_result = pd.merge(
            df_result, df_togrid, left_index=True, right_index=True
        )

        # production
        df_prod = pd.DataFrame(
            list(rs_production.get_points())[:-1]
        )
        df_prod = df_prod.set_index('time')

        df_result = pd.merge(
            df_result, df_prod, left_index=True, right_index=True
        )

        # consumption
        df_result['consumption'] = (
            df_result.fromgrid + (df_result.production - df_result.togrid)) * 12
        df_result['time'] = pd.to_datetime(df_result.index)
        df_result.consumption.describe()

        return TimePeriodStatsDto(
            start=df_result.time.min().astimezone(pytz.timezone('Europe/Brussels')),
            end=df_result.time.max().astimezone(pytz.timezone('Europe/Brussels')),
            unit='W',
            q25=df_result.consumption.quantile(0.25) * 1000,
            q50=df_result.consumption.quantile(0.5) * 1000,
            q75=df_result.consumption.quantile(0.75) * 1000,
            stddev=df_result.consumption.std() * 1000
        )

    @cache_for(seconds=900)
    def get_last_legionella_start(self):
        start = datetime.datetime.now().astimezone(
            pytz.utc) - datetime.timedelta(weeks=1)

        rs_tank_temp = self.client.query(
            f"""
            SELECT * FROM ecodan2_tank_temp where time >= '{start.isoformat()}'
            """
        )

        df = pd.DataFrame(
            list(rs_tank_temp.get_points())[:-1]
        )
        df['time_str'] = df.time
        df['time'] = pd.to_datetime(df.time)
        df = df.set_index('time')

        df = df[df.value >= 60]
        df['time_cyclus_should_end'] = df.index + \
            datetime.timedelta(minutes=20)
        df['time_cyclus_end'] = pd.to_datetime(df.time_str.shift(periods=-40))

        df = df[df['time_cyclus_should_end'] ==
                df['time_cyclus_end']].sort_index()

        if len(df) > 0:
            last_legionella = df.iloc[-1]

            return TimeDataDto(
                timestamp=last_legionella.name.astimezone(
                    pytz.timezone('Europe/Brussels')),
                value=last_legionella.value,
                unit='° C'
            )
        else:
            return TimeDataDto(
                timestamp=datetime.datetime(1970, 1, 1, 0, 0, 0, 0).astimezone(
                    pytz.timezone('Europe/Brussels')),
                value=-1,
                unit='° C'
            )

    @cache_for(seconds=5)
    def get_current_heatpump_status(self):
        start = datetime.datetime.now().astimezone(
            pytz.utc) - datetime.timedelta(minutes=2)

        operating_modes = {
            0: 'Stop',
            1: 'Hot water',
            2: 'Heating',
            3: 'Cooling',
            4: 'No voltage contact input (hot water storage)',
            5: 'Freeze stat',
            6: 'Legionella',
            7: 'Heating eco',
            8: 'Mode 1',
            9: 'Mode 2',
            10: 'Mode 3',
            11: 'No voltage contact input (heating up)'
        }

        rs_operating_mode = self.client.query(
            f"select * from ecodan2_operating_mode where time >= '{start.isoformat()}'order by time desc limit 1"
        )

        om = list(rs_operating_mode.get_points())
        if len(om) == 0:
            om = -99
        else:
            om = om[-1]['value']
        operating_mode = operating_modes.get(om, 'Unknown')

        heat_sources = {
            0: 'Heatpump',
            1: 'Immersion heater',
            2: 'Backup heater',
            3: 'Immersion and backup heater',
            4: 'Boiler'
        }

        rs_heat_source = self.client.query(
            f"select * from ecodan2_heat_source where time >= '{start.isoformat()}'order by time desc limit 1"
        )

        hs = list(rs_heat_source.get_points())
        if len(hs) == 0:
            hs = -99
        else:
            hs = hs[-1]['value']
        heat_source = heat_sources.get(hs, 'Unknown')

        if heat_source == 'Heatpump':
            rs_freq = self.client.query(
                f"select * from ecodan2_pump_freq where time >= '{start.isoformat()}'order by time desc limit 1"
            )

            freq = list(rs_freq.get_points())
            if len(freq) > 0:
                freq = freq[-1]['value']
                if freq == 0:
                    heat_source = 'Heatpump pause'

        defrost_statuses = {
            0: 'Normal',
            1: 'Standby',
            2: 'Defrost',
            3: 'Waiting restart'
        }

        rs_defrost_status = self.client.query(
            f"select * from ecodan2_defrost_status where time >= '{start.isoformat()}'order by time desc limit 1"
        )

        ds = list(rs_defrost_status.get_points())
        if len(ds) == 0:
            ds = -99
        else:
            ds = ds[-1]['value']
        defrost_status = defrost_statuses.get(ds, 'Unknown')

        return HeatPumpStatusDto(
            operating_mode=operating_mode,
            heat_source=heat_source,
            defrost_status=defrost_status
        )

    @cache_for(seconds=5)
    def get_heatpump_setpoint(self):
        start = datetime.datetime.now().astimezone(
            pytz.utc) - datetime.timedelta(minutes=2)

        rs_dhw_setpoint = self.client.query(
            f"select * from ecodan2_tank_set_temp where time >= '{start.isoformat()}'order by time desc limit 1"
        )

        dhw_setpoint = list(rs_dhw_setpoint.get_points())
        if len(dhw_setpoint) == 0:
            dhw_setpoint = None
        else:
            dhw_setpoint = dhw_setpoint[-1]['value']

        rs_heating_setpoint = self.client.query(
            f"select * from ecodan2_house_set_temp where time >= '{start.isoformat()}'order by time desc limit 1"
        )

        heating_setpoint = list(rs_heating_setpoint.get_points())
        if len(heating_setpoint) == 0:
            heating_setpoint = None
        else:
            heating_setpoint = heating_setpoint[-1]['value']

        return HeatPumpSetpointDto(
            dhw=dhw_setpoint,
            heating=heating_setpoint
        )

    @cache_for(seconds=5)
    def get_current_dhw_temp(self):
        start = datetime.datetime.now().astimezone(
            pytz.utc) - datetime.timedelta(minutes=2)

        rs_dhw_temp = self.client.query(
            f"select * from ecodan2_tank_temp where time >= '{start.isoformat()}'order by time desc limit 1"
        )

        results = []
        for r in rs_dhw_temp.get_points():
            r['time'] = to_brussels_time(
                datetime.datetime.strptime(r['time'], '%Y-%m-%dT%H:%M:%SZ'))
            results.append(r)

        results = sorted(results, key=lambda x: x['time'])

        if len(results) == 0:
            timestamp = None
            dhw_temp = None
        else:
            timestamp = results[-1]['time']
            dhw_temp = results[-1]['value']

        return TimeDataDto(
            timestamp=timestamp,
            value=dhw_temp,
            unit='° C'
        )
