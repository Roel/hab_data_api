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
import httpx
import pytz

from dto.generic import TimeDataDto


def to_brussels_time(datetime_utc):
    return datetime_utc.replace(tzinfo=pytz.utc).astimezone(
        pytz.timezone('Europe/Brussels'))


class GridDataClientElia:
    def __init__(self, app):
        self.app = app

        self.base_url = 'https://griddata.elia.be/eliabecontrols.prod/interface/Interconnections/daily/auctionresultsqh/'

        self.client = httpx.AsyncClient()

    async def get_grid_prices(self, date):
        date = date.strftime('%Y-%m-%d')
        url = f'{self.base_url}/{date}'

        grid_data = (await self.client.get(url)).json()
        return [TimeDataDto(
            timestamp=to_brussels_time(datetime.datetime.strptime(
                i['dateTime'], '%Y-%m-%dT%H:%M:%SZ')),
            value=i['price']/10,
            unit='c€/kWh') for i in grid_data]

    async def shutdown(self):
        await self.client.aclose()


class GridDataClientEntsoe:
    def __init__(self, app):
        self.app = app

        self.base_url = 'https://newtransparency.entsoe.eu/market/energyPrices/load'

        self.client = httpx.AsyncClient()

    async def get_grid_prices(self, date):
        period_from = datetime.datetime.combine(
            date, datetime.time(0, 0, 0)).astimezone(pytz.UTC)
        period_to = datetime.datetime.combine(
            date + datetime.timedelta(days=1), datetime.time(0, 0, 0)).astimezone(pytz.UTC)

        payload = {
            'areaList': ['BZN|10YBE----------2'],
            'dateTimeRange': {
                'from': period_from.isoformat(),
                'to': period_to.isoformat()
            },
            'filterMap': {},
            'intervalPageInfo': {
                'itemIndex': 0,
                'pageSize': 10
            },
            'timeZone': 'CET'
        }

        data = (await self.client.post(self.base_url, json=payload)).json()
        hourly_prices = data['instanceList'][0]['curveData']['periodList'][0]['pointMap']

        return [TimeDataDto(
            timestamp=to_brussels_time(
                period_from + datetime.timedelta(hours=int(hour))),
            value=float(hourly_prices[hour][0])/10.0,
            unit='c€/kWh') for hour in hourly_prices]

    async def shutdown(self):
        await self.client.aclose()
