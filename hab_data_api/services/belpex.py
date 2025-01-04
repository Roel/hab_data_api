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

import asyncio
import datetime


class BelpexService:
    def __init__(self, app):
        self.app = app
        self.client = self.app.clients.griddata

        self.__scheduled_jobs()

    async def update_grid_prices(self):
        last_grid_price = self.app.services.influx.get_last_grid_price()

        if last_grid_price is None:
            date_from = datetime.date.today() - datetime.timedelta(days=40)
        else:
            date_from = last_grid_price.timestamp.date() + \
                datetime.timedelta(days=1)

        date_to = datetime.date.today() + datetime.timedelta(days=1)

        date_to_fetch = date_from
        while date_to_fetch <= date_to:
            data = await self.client.get_grid_prices(date_to_fetch)
            self.app.services.influx.save_grid_prices(data)
            date_to_fetch = date_to_fetch + datetime.timedelta(days=1)

    def __scheduled_jobs(self):
        self.app.scheduler.add_job(
            self.update_grid_prices, 'cron', hour='12-23', minute='47')
