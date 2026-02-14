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

import datetime
import math
from quart import Blueprint, request, current_app as app
from quart_auth import basic_auth_required

from dto.generic import TimeDataInterpolatedRangeDto, TimeDataDto

api = Blueprint('api', __name__)

TYPEFN_DATE = datetime.date.fromisoformat
TYPEFN_DATETIME = datetime.datetime.fromisoformat


@api.get("/power/fromgrid/current")
@basic_auth_required()
async def get_current_power_fromgrid():
    result = app.services.influx.get_current_power_fromgrid()

    return {
        'timestamp': result.timestamp.isoformat(),
        'value': result.value,
        'unit': result.unit
    }


@api.get("/power/net/current")
@basic_auth_required()
async def get_current_power_net():
    result = app.services.influx.get_current_power_net()

    return {
        'timestamp': result.timestamp.isoformat(),
        'value': result.value,
        'unit': result.unit
    }


@api.get("/production/current")
@basic_auth_required()
async def get_current_production():
    result = app.services.influx.get_current_production()

    return {
        'timestamp': result.timestamp.isoformat(),
        'value': result.value,
        'unit': result.unit
    }


@api.get("/production/daily")
@basic_auth_required()
async def get_daily_production():
    errors = []

    date = request.args.get(
        'date', default=datetime.date.today(), type=TYPEFN_DATE)
    if date is None:
        errors.append('Failed to parse value for parameter: date.')

    if len(errors) == 0:
        result = app.services.influx.get_daily_production(date=date)
        return {'timestamp': result.timestamp.isoformat(),
                'value': result.value,
                'unit': result.unit}
    else:
        return {'status': 'error',
                'errors': errors}, 400


@api.get("/consumption/current")
@basic_auth_required()
async def get_current_consumption():
    result = app.services.influx.get_current_consumption()

    return {
        'timestamp': result.timestamp.isoformat(),
        'value': result.value,
        'unit': result.unit
    }


@api.get("/consumption/baseline")
@basic_auth_required()
async def get_baseline_consumption():
    result = app.services.influx.get_baseline_consumption()

    return {
        'start': result.start.isoformat(),
        'end': result.end.isoformat(),
        'unit': result.unit,
        'q25': result.q25,
        'q50': result.q50,
        'q75': result.q75,
        'stddev': result.stddev
    }


@api.get("/legionella/last")
@basic_auth_required()
async def get_last_legionella_start():
    result = app.services.influx.get_last_legionella_start()

    return {
        'timestamp': result.timestamp.isoformat(),
        'value': result.value,
        'unit': result.unit
    }


@api.get("/dhw/temp")
@basic_auth_required()
async def get_current_dhw_temp():
    result = app.services.influx.get_current_dhw_temp()

    return {
        'timestamp': result.timestamp.isoformat(),
        'value': result.value,
        'unit': result.unit
    }

@api.get("/outside/temp")
@basic_auth_required()
async def get_current_outside_temp():
    result = app.services.influx.get_current_outside_temp()

    return {
        'timestamp': result.timestamp.isoformat(),
        'value': result.value,
        'unit': result.unit
    }

@api.get("/heatpump/status")
@basic_auth_required()
async def get_current_heatpump_status():
    result = app.services.influx.get_current_heatpump_status()

    return {
        'operating_mode': result.operating_mode,
        'heat_source': result.heat_source,
        'defrost_status': result.defrost_status
    }


@api.get("/heatpump/setpoint")
@basic_auth_required()
async def get_heatpump_setpoint():
    result = app.services.influx.get_heatpump_setpoint()

    return {
        'dhw': result.dhw,
        'heating': result.heating
    }


@api.get("/house/temp")
@basic_auth_required()
async def get_house_temperature():
    end = request.args.get(
        'end', default=datetime.datetime.now(), type=TYPEFN_DATETIME)
    start = request.args.get(
        'start', default=(end - datetime.timedelta(days=1)), type=TYPEFN_DATETIME)

    result = app.services.influx.get_house_temperature(start, end)

    return {
        'start': result.start.isoformat(),
        'end': result.end.isoformat(),
        'unit': result.unit,
        'q25': result.q25,
        'q50': result.q50,
        'q75': result.q75,
        'stddev': result.stddev
    }


@api.post("/price/simulate/total")
@basic_auth_required()
async def simulate_price():
    data = await request.json

    interpolation_method = data.get("interpolation_method", None) or "pchip"
    timedata = []

    for t in data.get("data", []):
        timestamp = t.get("timestamp", None)
        if timestamp is None:
            continue
        else:
            timestamp = TYPEFN_DATETIME(timestamp)

        timedata.append(
            TimeDataDto(
                timestamp=timestamp.replace(
                    minute=math.floor(timestamp.minute / 15) * 15, second=0
                ),
                value=t.get("net_power"),
                unit="W",
            )
        )

    dto = TimeDataInterpolatedRangeDto(interpolation_method, timedata)
    input_df = dto.to_df("15min")

    try:
        result = app.services.price.simulate_aggregated_price_total(input_df)

        return {
            "start": result.start.isoformat(),
            "end": result.end.isoformat(),
            "unit": result.unit,
            "q25": result.q25,
            "q50": result.q50,
            "q75": result.q75,
            "stddev": result.stddev,
            "sum": result.sum,
        }
    except RuntimeError as e:
        return {"error": e.args[0]}, 400


@api.post("/price/simulate/total/detail")
@basic_auth_required()
async def simulate_price_detail():
    data = await request.json

    interpolation_method = data.get("interpolation_method", None) or "pchip"
    timedata = []

    for t in data.get("data", []):
        timestamp = t.get("timestamp", None)
        if timestamp is None:
            continue
        else:
            timestamp = TYPEFN_DATETIME(timestamp)

        timedata.append(
            TimeDataDto(
                timestamp=timestamp.replace(
                    minute=math.floor(timestamp.minute / 15) * 15, second=0
                ),
                value=t.get("net_power"),
                unit="W",
            )
        )

    dto = TimeDataInterpolatedRangeDto(interpolation_method, timedata)
    input_df = dto.to_df("15min")

    try:
        result = app.services.price.simulate_aggregated_price_total_detail(input_df)

        return [
            {"timestamp": x.timestamp.isoformat(), "value": x.value, "unit": x.unit}
            for x in result
        ]
    except RuntimeError as e:
        return {"error": e.args[0]}, 400
