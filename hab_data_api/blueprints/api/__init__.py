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

from quart import Blueprint, current_app as app
from quart_auth import basic_auth_required

api = Blueprint('api', __name__)


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
