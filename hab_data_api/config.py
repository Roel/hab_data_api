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

import os


def read_secret(variable_name):
    if f'{variable_name}_FILE' in os.environ:
        with open(os.environ.get(f'{variable_name}_FILE'), 'r') as secret_file:
            secret = secret_file.read()
    else:
        secret = os.environ.get(variable_name, None)
    return secret


class Config:
    QUART_AUTH_MODE = 'bearer'
    QUART_AUTH_BASIC_USERNAME = 'admin'
    QUART_AUTH_BASIC_PASSWORD = read_secret('API_ADMIN_PASS')

    INFLUX_HOST = os.environ.get('INFLUX_HOST')
    INFLUX_DATABASE = os.environ.get('INFLUX_DATABASE')
    INFLUX_USERNAME = os.environ.get('INFLUX_USERNAME')
    INFLUX_PASSWORD = read_secret('INFLUX_PASSWORD')

    GRIDDATA_CLIENT = os.environ.get('GRIDDATA_CLIENT')
