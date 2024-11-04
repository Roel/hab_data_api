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
import pytz

from quart import Blueprint, request, current_app as app
from quart_auth import basic_auth_required

grafana = Blueprint('grafana', __name__)


def get_range(data):
    date_from = pytz.utc.localize(datetime.datetime.strptime(
        data['range']['from'][:-5], '%Y-%m-%dT%H:%M:%S')).astimezone(pytz.timezone('Europe/Brussels'))

    date_to = pytz.utc.localize(datetime.datetime.strptime(
        data['range']['to'][:-5], '%Y-%m-%dT%H:%M:%S')).astimezone(pytz.timezone('Europe/Brussels'))

    return date_from, date_to


def get_targets(data):
    return [i['target'] for i in data['targets']]


@grafana.get("/")
@basic_auth_required()
async def test_connection():
    return {'status': 'ok'}, 200


@grafana.post("/metrics")
@basic_auth_required()
async def get_metrics():
    return [
        {"label": "Heatpump status", "value": "heatpump_status"},
        {"label": "Baseline consumption", "value": "baseline_consumption"}
    ]


@grafana.post("/metric-payload-options")
@basic_auth_required()
async def get_metric_payload_options():
    return []


@grafana.post("/query")
@basic_auth_required()
async def query():
    data = await request.json

    date_from, date_to = get_range(data)
    targets = get_targets(data)

    date_from = date_from - datetime.timedelta(minutes=10)
    date_to = date_to + datetime.timedelta(minutes=10)

    now = int(datetime.datetime.now().strftime('%s'))*1000

    result = []

    for t in targets:
        if t == 'heatpump_status':
            hs = app.services.influx.get_current_heatpump_status()

            operating_modes = {
                'Stop': '⏻',
                'Heating': '🏡',
                'Heating eco': '🛖',
                'Hot water': '🛀',
                'Freeze stat': '❄',
                'Legionella': '🌶'
            }

            heat_sources = {
                'Heatpump': '✇',
                'Heatpump pause': '⏼︎',
                'Immersion heater': '⚡',
                'Backup heater': '⚡',
                'Immersion and backup heater': '⚡',
                'Boiler': '🔥'
            }

            defrost_statuses = {
                'Standby': '⏼︎❄',
                'Defrost': '❄',
                'Waiting restart': '⏼︎❄'
            }

            r = ''
            if hs.defrost_status != 'Normal':
                r = defrost_statuses.get(hs.defrost_status)
            elif hs.operating_mode == 'Stop':
                r = operating_modes.get(hs.operating_mode)
            elif hs.heat_source == 'Heatpump pause':
                r = heat_sources.get(hs.heat_source)
            else:
                r += operating_modes.get(hs.operating_mode, '?')
                r += ' ' + heat_sources.get(hs.heat_source, '?')

            datapoints = [[r, now]]

            result.append({
                'target': 'heatpump_status',
                'datapoints': datapoints
            })
        elif t == 'baseline_consumption':
            bc = app.services.influx.get_baseline_consumption()
            bc = bc.q50 + (1.5 * bc.stddev)

            datapoints = [
                [bc, int(date_from.strftime('%s'))*1000],
                [bc, int(date_to.strftime('%s'))*1000]
            ]

            result.append({
                'target': 'baseline_consumption',
                'datapoints': datapoints
            })

    return result
