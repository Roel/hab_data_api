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

import glob
import importlib.util
import os
from pathlib import Path
import sys

import sqlite3
import aiosqlite


class Database:
    def __init__(self, app):
        self.app = app

        Model.db = self

        self.db_path = self.app.config['DATABASE_PATH']

    def connect(self):
        return aiosqlite.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)

    async def migrate(self):
        async with self.connect() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                        name primary key
                );
            """)
            await conn.commit()

        migrations_dir = os.path.join(os.path.dirname(__file__), 'migrations')
        for m in glob.glob(f'{migrations_dir}/*.py'):
            name = Path(m).name

            async with self.connect() as conn:
                async with conn.execute("SELECT count() FROM migrations WHERE name = :name",
                                        {'name': name}) as curs:
                    if (await curs.fetchone())[0] < 1:
                        spec = importlib.util.spec_from_file_location(
                            "ecodan.db.migration", m)
                        mod = importlib.util.module_from_spec(spec)
                        sys.modules["ecodan.db.migration"] = mod
                        spec.loader.exec_module(mod)

                        async with self.connect() as conn:
                            await mod.migrate(conn)
                            await conn.execute("INSERT INTO migrations VALUES (:name)", {'name': name})
                            await conn.commit()


class Model:
    db = None
