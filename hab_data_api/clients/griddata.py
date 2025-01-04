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


class GridDataClient:
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
