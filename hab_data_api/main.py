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
import logging

from quart import Quart
from quart_auth import QuartAuth

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import Config

from clients.influx import InfluxClient
from clients.griddata import GridDataClient

from services.cache import CacheService
from services.influx import InfluxService
from services.belpex import BelpexService
from services.price import PriceService

from blueprints.status import status
from blueprints.api import api
from blueprints.grafana import grafana


class Clients:
    def __init__(self, app):
        self.app = app

        self.influx = InfluxClient(
            self.app,
            host=self.app.config['INFLUX_HOST'],
            database=self.app.config['INFLUX_DATABASE'],
            username=self.app.config['INFLUX_USERNAME'],
            password=self.app.config['INFLUX_PASSWORD'])

        self.griddata = GridDataClient(self.app)

    async def shutdown(self):
        await asyncio.gather(
            self.griddata.shutdown()
        )


class Services:
    def __init__(self, app):
        self.app = app

        self.cache = CacheService(self.app)
        self.influx = InfluxService(self.app)
        self.belpex = BelpexService(self.app)
        self.price = PriceService(self.app)


class Logger:
    def __init__(self, app):
        self.app = app
        self.logger = logging.getLogger('hab_data_api')
        hdlr = logging.StreamHandler()
        hdlr.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.DEBUG)

    def log(self, *args, **kwargs):
        return self.logger.log(*args, **kwargs)

    def debug(self, message):
        return self.log(logging.DEBUG, message)

    def info(self, message):
        return self.log(logging.INFO, message)

    def warning(self, message):
        return self.log(logging.WARNING, message)

    def error(self, message):
        return self.log(logging.ERROR, message)


app = Quart(__name__)
app.config.from_object(Config)
app.secret_key = app.config['SECRET_KEY']

app.auth = QuartAuth(app)
app.log = Logger(app)


@app.before_serving
async def startup():
    loop = asyncio.get_event_loop()

    app.scheduler = AsyncIOScheduler(event_loop=loop)
    app.scheduler.start()

    app.clients = Clients(app)
    app.services = Services(app)

    app.register_blueprint(status, url_prefix='/status')
    app.register_blueprint(api, url_prefix='/api')
    app.register_blueprint(grafana, url_prefix='/grafana')


@app.after_serving
async def shutdown():
    app.scheduler.shutdown()

    await app.clients.shutdown()
