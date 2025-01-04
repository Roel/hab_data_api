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

import pandas as pd

import calendar
import datetime
from dateutil.relativedelta import relativedelta


class PriceService:
    def __init__(self, app):
        self.app = app

        self.price_calculation = {
            2024: PriceCalculationWaseWind2024(self.app),
            2025: PriceCalculationWaseWind2025(self.app)
        }

    def get_aggregated_price(self, start_date, end_date, freq):
        date_range = [start_date, end_date]
        year_ends = [i.to_pydatetime().date() + datetime.timedelta(days=1) for i in pd.date_range(
            start_date, end_date, freq='A')]  # YE
        year_ends *= 2
        date_range.extend(year_ends)
        date_range = sorted(date_range)

        result = pd.DataFrame()

        for i in range(int(len(date_range) / 2)):
            start_date = date_range[i*2]
            end_date = date_range[(i*2)+1]

            if start_date == end_date:
                continue

            calculation = self.price_calculation.get(start_date.year, None)

            if calculation is None:
                raise ValueError(
                    f'No price calculation exists for the year {start_date.year}')

            price = calculation.get_aggregated_price(
                start_date, end_date, freq)

            if price is not None:
                result = pd.concat([result, price])

        return result

    def get_monthly_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, 'MS')

    def get_daily_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, '1D')

    def get_hourly_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, '1H')


class AbstractPriceCalculation:
    def __init__(self, app):
        self.app = app

    def get_invoice_peak(self, year, month):
        """Get the invoice peak for the given year and month."""
        return self.app.services.influx.get_invoice_peak(year, month)

    def get_hourly_energy_consumption_injection(self, start_date, end_date):
        """Get the montly energy consumption and injection between given start date and end date."""
        return self.app.services.influx.get_hourly_energy_consumption_injection(start_date, end_date)

    def get_consumption_rate1_price(self, timestamp):
        """Get the price per kWh for rate1 (high) consumption for the given timestamp."""
        raise NotImplementedError

    def get_consumption_rate2_price(self, timestamp):
        """Get the price per kWh for rate2 (low) consumption for the given timestamp."""
        raise NotImplementedError

    def get_injection_rate1_price(self, timestamp):
        """Get the price per kWh for rate1 (high) injection for the given timestamp."""
        raise NotImplementedError

    def get_injection_rate2_price(self, timestamp):
        """Get the price per kWh for rate2 (low) consumption for the given timestamp."""
        raise NotImplementedError

    def get_subscription_price(self):
        """Get the subscription from energy provider price per year for the given year."""
        raise NotImplementedError

    def get_distribution_price_per_kW_peak(self):
        """Get the distribution price per month per kW invoice peak for the given year and month."""
        raise NotImplementedError

    def get_distribution_price_per_kWh(self):
        """Get the distribution price per kWh for the given year and month."""
        raise NotImplementedError

    def get_distribution_price_fixed(self):
        """Get the fixed distribution price per month for the given year and month."""
        raise NotImplementedError

    def get_eneryfund_price(self):
        """Get the energyfund price per year for the given year."""
        raise NotImplementedError

    def get_aggregated_price(self, start_date, end_date, freq):
        energy_stats = self.get_hourly_energy_consumption_injection(
            start_date, end_date)

        if energy_stats is not None:
            energy_stats = energy_stats.apply(
                self.calculate_price, axis=1, result_type='expand')

            return energy_stats.groupby(pd.Grouper(freq=freq)).agg('sum')

    def get_monthly_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, 'MS')

    def get_daily_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, '1D')

    def get_hourly_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, '1H')

    def calculate_price(self, df_row):
        timestamp = df_row.name

        hours_in_year = 365 * 24
        if calendar.isleap(timestamp.year):
            hours_in_year += 24

        days_in_month = calendar.monthrange(timestamp.year, timestamp.month)[1]
        hours_in_month = days_in_month * 24

        fixed_component = self.get_subscription_price() / hours_in_year
        fixed_component += self.get_eneryfund_price() / hours_in_year
        fixed_component += self.get_distribution_price_fixed() / hours_in_month

        distrib_peak_component = self.get_distribution_price_per_kW_peak(
        ) * self.get_invoice_peak(timestamp.year, timestamp.month) / hours_in_year

        distrib_dynamic_component = self.get_distribution_price_per_kWh() * (
            df_row.consumption_rate1 + df_row.consumption_rate2
        )

        consumption_price = self.get_consumption_rate1_price(
            timestamp) * df_row.consumption_rate1
        consumption_price += self.get_consumption_rate2_price(
            timestamp) * df_row.consumption_rate2

        injection_price = self.get_injection_rate1_price(
            timestamp) * df_row.injection_rate1
        injection_price += self.get_injection_rate2_price(
            timestamp) * df_row.injection_rate2

        total = (fixed_component + distrib_peak_component +
                 distrib_dynamic_component + consumption_price - injection_price)

        return pd.Series(
            [fixed_component, distrib_peak_component, distrib_dynamic_component,
             consumption_price, -injection_price, total],
            index=['fixed', 'peak', 'distribution', 'consumption', 'injection', 'total'])


class AbstractDynamicPriceCalculation(AbstractPriceCalculation):
    def get_monthly_belpex(self, timestamp):
        """Get mean belpex day ahead price in c€/kWh for the given month and year."""
        return self.app.services.influx.get_monthly_belpex(timestamp.year, timestamp.month)


class PriceCalculationWaseWind2024(AbstractDynamicPriceCalculation):
    def get_consumption_rate1_price(self, timestamp):
        return (0.115 * 0.5 * self.get_monthly_belpex(timestamp) * 10 + 7.46) / 100

    def get_consumption_rate2_price(self, timestamp):
        return (0.100 * 0.5 * self.get_monthly_belpex(timestamp) * 10 + 6.63) / 100

    def get_injection_rate1_price(self, timestamp):
        return (0.08 * self.get_monthly_belpex(timestamp) * 10 - 0.6) / 100

    def get_injection_rate2_price(self, timestamp):
        return (0.06 * self.get_monthly_belpex(timestamp) * 10 - 0.6) / 100

    def get_subscription_price(self):
        return 60

    def get_distribution_price_per_kW_peak(self):
        return 37.15

    def get_distribution_price_per_kWh(self):
        distributie_per_kWh = 0.0098665
        openbaredienst_verplichtingen = 0.0229011
        toeslagen = 0.0010861
        overige_transmissienetkosten = 0.0043571
        certificaten = 0.015667

        accijnzen = 0.0494061

        return (
            (distributie_per_kWh + openbaredienst_verplichtingen + toeslagen +
             overige_transmissienetkosten + accijnzen) * 1.06) \
            + certificaten

    def get_distribution_price_fixed(self):
        databeheer = 15.09
        return databeheer / 12.0

    def get_eneryfund_price(self):
        return 0


class PriceCalculationWaseWind2025(AbstractDynamicPriceCalculation):
    def get_consumption_rate1_price(self, timestamp):
        return (0.115 * 0.5 * self.get_monthly_belpex(timestamp) * 10 + 7.16) / 100

    def get_consumption_rate2_price(self, timestamp):
        return (0.100 * 0.5 * self.get_monthly_belpex(timestamp) * 10 + 6.36) / 100

    def get_injection_rate1_price(self, timestamp):
        return (0.07 * self.get_monthly_belpex(timestamp) * 10 - 1) / 100

    def get_injection_rate2_price(self, timestamp):
        return (0.05 * self.get_monthly_belpex(timestamp) * 10 - 1) / 100

    def get_subscription_price(self):
        return 65

    def get_distribution_price_per_kW_peak(self):
        return 49.0426291 * 1.06

    def get_distribution_price_per_kWh(self):
        distributie_per_kWh = 0.0236764
        openbaredienst_verplichtingen = 0.0277220
        toeslagen = 0.0014996
        overige_transmissienetkosten = 0  # 0.0043571
        certificaten = 0.01567

        accijnzen = 0.04748

        return (
            (distributie_per_kWh + openbaredienst_verplichtingen + toeslagen +
             overige_transmissienetkosten + accijnzen) * 1.06) \
            + certificaten

    def get_distribution_price_fixed(self):
        databeheer = 17.51 * 1.06
        return databeheer / 12.0

    def get_eneryfund_price(self):
        return 0
