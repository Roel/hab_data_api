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

from dateutil.relativedelta import relativedelta

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
        {"label": "Baseline consumption", "value": "baseline_consumption"},
        {"label": "Electricity price last 10 days (daily)", "value": "price_daily"},
        {"label": "Electricity price (hourly)", "value": "price_hourly"},
        {"label": "Electricity price this month", "value": "price_this_month"},
        {"label": "Electricity invoice peak this month", "value": "invoice_peak"},
        {"label": "Electricity peak this month", "value": "current_month_peak"},
        {
            "label": "Electricity price detail this month",
            "value": "price_detail_this_month",
        },
        {
            "label": "Electricity price detail previous month",
            "value": "price_detail_previous_month",
        },
        {"label": "Belpex this month", "value": "belpex_this_month"},
        {"label": "Belpex previous month", "value": "belpex_previous_month"},
        {
            "label": "Alternative electricity price last 10 days (daily)",
            "value": "price2_daily",
        },
        {"label": "Alternative electricity price (hourly)", "value": "price2_hourly"},
        {
            "label": "Alternative electricity price this month",
            "value": "price2_this_month",
        },
        {
            "label": "Alternative electricity price detail this month",
            "value": "price2_detail_this_month",
        },
        {
            "label": "Alternative electricity price detail previous month",
            "value": "price2_detail_previous_month",
        },
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
            bc = bc.q50 + (1.5 * bc.stddev) / 1000

            datapoints = [
                [bc, int(date_from.strftime('%s'))*1000],
                [bc, int(date_to.strftime('%s'))*1000]
            ]

            result.append({
                'target': 'baseline_consumption',
                'datapoints': datapoints
            })
        elif t == 'price_hourly':
            start_date = date_from.date()
            end_date = date_to.date() + datetime.timedelta(days=1)

            df = app.services.price.get_hourly_price(start_date, end_date)

            datapoints = [
                [i.total, int(i.Index.strftime('%s'))*1000] for i in df.itertuples()
            ]

            result.append({
                'target': 'price_hourly',
                'datapoints': datapoints
            })
        elif t == 'price_daily':
            start_date = date_to.date() - datetime.timedelta(days=10)
            end_date = date_to.date() + datetime.timedelta(days=1)

            df = app.services.price.get_daily_price(start_date, end_date)

            datapoints = [
                [i.total, int(i.Index.strftime('%s'))*1000] for i in df.itertuples()
            ]

            result.append({
                'target': 'price_daily',
                'datapoints': datapoints
            })
        elif t == 'price_this_month':
            start_date = datetime.date(date_to.year, date_to.month, 1)
            end_date = start_date + relativedelta(months=1)

            df = app.services.price.get_monthly_price(start_date, end_date)

            datapoints = [
                [i.total, int(i.Index.strftime('%s'))*1000] for i in df.itertuples()
            ]

            result.append({
                'target': 'price_this_month',
                'datapoints': datapoints
            })
        elif t == 'price_detail_this_month':
            start_date = datetime.date(date_to.year, date_to.month, 1)
            end_date = start_date + relativedelta(months=1)

            df = app.services.price.get_monthly_price(start_date, end_date)
            details = list(df)

            for i in details:
                result.append({
                    'target': i,
                    'datapoints': [[df.iloc[0][i], int(start_date.strftime('%s'))*1000]]
                })
        elif t == 'price_detail_previous_month':
            start_date = datetime.date(
                date_to.year, date_to.month, 1) - relativedelta(months=1)
            end_date = datetime.date(date_to.year, date_to.month, 1)

            df = app.services.price.get_monthly_price(start_date, end_date)
            details = list(df)

            for i in details:
                result.append({
                    'target': i,
                    'datapoints': [[df.iloc[0][i], int(start_date.strftime('%s'))*1000]]
                })
        elif t == 'current_month_peak':
            peak = app.services.influx.get_current_month_peak()

            datapoints = [
                [peak, int(date_from.strftime('%s'))*1000],
                [peak, int(date_to.strftime('%s'))*1000]
            ]

            result.append({
                'target': 'current_month_peak',
                'datapoints': datapoints
            })
        elif t == 'invoice_peak':
            date = datetime.date(date_to.year, date_to.month, 1)
            peak = app.services.influx.get_invoice_peak(
                date_to.year, date_to.month)

            datapoints = [
                [peak, int(date.strftime('%s'))*1000]
            ]

            result.append({
                'target': 'invoice_peak',
                'datapoints': datapoints
            })
        elif t == 'belpex_this_month':
            date = datetime.date(date_to.year, date_to.month, 1)
            belpex = app.services.influx.get_monthly_belpex(
                date.year, date.month)

            datapoints = [
                [belpex, int(date.strftime('%s'))*1000]
            ]

            result.append({
                'target': 'belpex_this_month',
                'datapoints': datapoints
            })
        elif t == 'belpex_previous_month':
            date = datetime.date(date_to.year, date_to.month, 1)
            date = date - relativedelta(months=1)
            belpex = app.services.influx.get_monthly_belpex(
                date.year, date.month)

            datapoints = [
                [belpex, int(date.strftime('%s'))*1000]
            ]

            result.append({
                'target': 'belpex_previous_month',
                'datapoints': datapoints
            })
        elif t == "price2_hourly":
            start_date = date_from.date()
            end_date = date_to.date() + datetime.timedelta(days=1)

            df = app.services.alternative_price.get_hourly_price(start_date, end_date)

            datapoints = [
                [i.total, int(i.Index.strftime("%s")) * 1000] for i in df.itertuples()
            ]

            result.append({"target": "price2_hourly", "datapoints": datapoints})
        elif t == "price2_daily":
            start_date = date_to.date() - datetime.timedelta(days=10)
            end_date = date_to.date() + datetime.timedelta(days=1)

            df = app.services.alternative_price.get_daily_price(start_date, end_date)

            datapoints = [
                [i.total, int(i.Index.strftime("%s")) * 1000] for i in df.itertuples()
            ]

            result.append({"target": "price2_daily", "datapoints": datapoints})
        elif t == "price2_this_month":
            start_date = datetime.date(date_to.year, date_to.month, 1)
            end_date = start_date + relativedelta(months=1)

            df = app.services.alternative_price.get_monthly_price(start_date, end_date)

            datapoints = [
                [i.total, int(i.Index.strftime("%s")) * 1000] for i in df.itertuples()
            ]

            result.append({"target": "price2_this_month", "datapoints": datapoints})
        elif t == "price2_detail_this_month":
            start_date = datetime.date(date_to.year, date_to.month, 1)
            end_date = start_date + relativedelta(months=1)

            df = app.services.alternative_price.get_monthly_price(start_date, end_date)
            details = list(df)

            for i in details:
                result.append(
                    {
                        "target": i,
                        "datapoints": [
                            [df.iloc[0][i], int(start_date.strftime("%s")) * 1000]
                        ],
                    }
                )
        elif t == "price2_detail_previous_month":
            start_date = datetime.date(date_to.year, date_to.month, 1) - relativedelta(
                months=1
            )
            end_date = datetime.date(date_to.year, date_to.month, 1)

            df = app.services.alternative_price.get_monthly_price(start_date, end_date)
            details = list(df)

            for i in details:
                result.append(
                    {
                        "target": i,
                        "datapoints": [
                            [df.iloc[0][i], int(start_date.strftime("%s")) * 1000]
                        ],
                    }
                )

    return result
