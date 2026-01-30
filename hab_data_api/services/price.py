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


class PriceService:
    def __init__(self, app):
        self.app = app

        self.price_calculation = {
            2024: {
                (datetime.date(2024, 1, 1), datetime.date(2025, 1, 1)): PriceCalculationWaseWind2024(self.app),
            },
            2025: {
                (datetime.date(2025, 1, 1), datetime.date(2026, 1, 1)): PriceCalculationWaseWind2025(self.app)
            },
            2026: {
                (datetime.date(2026, 1, 1), datetime.date(2026, 2, 1)): PriceCalculationWaseWind2026(self.app),
                (datetime.date(2026, 2, 1), datetime.date(2027, 1, 1)): PriceCalculationWaseWindDynamic2026(self.app)
            }
        }

    def get_aggregated_price(self, start_date, end_date, freq):
        """
        Get aggregated prices with support for multiple calculations per year.
        """
        # Ensure dates are datetime objects
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        result = pd.DataFrame()

        # Collect unique years in the date range
        years_to_calculate = sorted(set(range(start_date.year, end_date.year + 1)))

        for year in years_to_calculate:
            # Check if the year has any calculations defined
            year_calculations = self.price_calculation.get(year, {})

            if not year_calculations:
                raise ValueError(f'No price calculations exist for the year {year}')

            # Find calculations that overlap with the requested date range
            for (calc_start, calc_end), calculation in year_calculations.items():
                # Calculate the intersection of date ranges
                intersection_start = max(start_date, calc_start)
                intersection_end = min(end_date, calc_end)

                # Skip if no overlap
                if intersection_start > intersection_end:
                    continue

                # Get the aggregated price for the overlapping period
                price = calculation.get_aggregated_price(
                    intersection_start, 
                    intersection_end, 
                    freq
                )

                if price is not None and not price.empty:
                    result = pd.concat([result, price])

        # Check if any prices were calculated
        if result.empty:
            return None

        return result

    def get_monthly_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, 'MS')

    def get_daily_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, '1D')

    def get_hourly_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, '1h')


class AlternativePriceService(PriceService):
    def __init__(self, app):
        super().__init__(app)

        self.price_calculation = {
            2025: PriceCalculationWaseWind2026(self.app),
            2026: PriceCalculationWaseWindDynamic2026(self.app),
        }

        self.price_calculation = {
            2025: {
                (datetime.date(2025, 1, 1), datetime.date(2026, 1, 1)): PriceCalculationWaseWind2026(self.app)
            },
            2026: {
                (datetime.date(2026, 1, 1), datetime.date(2026, 2, 1)): PriceCalculationWaseWindDynamic2026(self.app),
                (datetime.date(2026, 2, 1), datetime.date(2027, 1, 1)): PriceCalculationWaseWind2026(self.app)
            }
        }


class AbstractPriceCalculation:
    def __init__(self, app):
        self.app = app

    def get_invoice_peak(self, year, month):
        """Get the invoice peak for the given year and month."""
        return self.app.services.influx.get_invoice_peak(year, month)

    def get_hourly_energy_consumption_injection(self, start_date, end_date):
        """Get the hourly energy consumption and injection between given start date and end date."""
        return self.app.services.influx.get_hourly_energy_consumption_injection(start_date, end_date)

    def get_15minutely_energy_consumption_injection(self, start_date, end_date):
        """Get the 15-minutely energy consumption and injection between given start date and end date."""
        return self.app.services.influx.get_15minutely_energy_consumption_injection(
            start_date, end_date
        )

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
        energy_stats = self.get_15minutely_energy_consumption_injection(
            start_date, end_date
        )

        if energy_stats is not None:
            energy_stats = energy_stats.apply(
                self.calculate_price, axis=1, result_type='expand')

            return energy_stats.groupby(pd.Grouper(freq=freq)).agg('sum')

    def get_monthly_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, 'MS')

    def get_daily_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, '1D')

    def get_hourly_price(self, start_date, end_date):
        return self.get_aggregated_price(start_date, end_date, '1h')

    def calculate_price(self, df_row):
        timestamp = df_row.name

        quarter_hours_in_year = 365 * 24 * 4
        if calendar.isleap(timestamp.year):
            quarter_hours_in_year += 24 * 4

        days_in_month = calendar.monthrange(timestamp.year, timestamp.month)[1]
        quarter_hours_in_month = days_in_month * 24 * 4

        fixed_component = self.get_subscription_price() / quarter_hours_in_year
        fixed_component += self.get_eneryfund_price() / quarter_hours_in_year
        fixed_component += self.get_distribution_price_fixed() / quarter_hours_in_month

        distrib_peak_component = (
            self.get_distribution_price_per_kW_peak()
            * self.get_invoice_peak(timestamp.year, timestamp.month)
            / quarter_hours_in_year
        )

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

    def get_belpex(self, timestamp):
        return self.app.services.influx.get_belpex(timestamp)


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
        return 51.99

    def get_distribution_price_per_kWh(self):
        distributie = 0.0561
        certificaten = 0.0157
        accijnzen = 0.0523

        return distributie + certificaten + accijnzen

    def get_distribution_price_fixed(self):
        databeheer = 18.52
        return databeheer / 12.0

    def get_eneryfund_price(self):
        return 0


class PriceCalculationWaseWind2026(AbstractDynamicPriceCalculation):
    def get_consumption_rate1_price(self, timestamp):
        return (0.138 * 0.5 * self.get_monthly_belpex(timestamp) * 10 + 7.72) / 100

    def get_consumption_rate2_price(self, timestamp):
        return self.get_consumption_rate1_price(timestamp)

    def get_injection_rate1_price(self, timestamp):
        return 0.02

    def get_injection_rate2_price(self, timestamp):
        return self.get_injection_rate1_price(timestamp)

    def get_subscription_price(self):
        return 65

    def get_distribution_price_per_kW_peak(self):
        return 50.1239818 * 1.06

    def get_distribution_price_per_kWh(self):
        distributie = 0.0248638 * 1.06
        openbare_dienst = 0.0236385 * 1.06
        toeslagen = 0.0013038 * 1.06
        certificaten = (1.13 + 0.33) / 100
        accijnzen = 0.0503288 + 0.0020417

        return distributie + openbare_dienst + toeslagen + certificaten + accijnzen

    def get_distribution_price_fixed(self):
        databeheer = 18.921
        return databeheer / 12.0

    def get_eneryfund_price(self):
        return 0


class PriceCalculationWaseWindDynamic2026(AbstractDynamicPriceCalculation):
    def get_consumption_rate1_price(self, timestamp):
        return (0.106 * 0.5 * self.get_belpex(timestamp) * 10 + 7.72) / 100

    def get_consumption_rate2_price(self, timestamp):
        return self.get_consumption_rate1_price(timestamp)

    def get_injection_rate1_price(self, timestamp):
        return 0.02

    def get_injection_rate2_price(self, timestamp):
        return self.get_injection_rate1_price(timestamp)

    def get_subscription_price(self):
        return 65

    def get_distribution_price_per_kW_peak(self):
        return 50.1239818 * 1.06

    def get_distribution_price_per_kWh(self):
        distributie = 0.0248638 * 1.06
        openbare_dienst = 0.0236385 * 1.06
        toeslagen = 0.0013038 * 1.06
        certificaten = (1.13 + 0.33) / 100
        accijnzen = 0.0503288 + 0.0020417

        return distributie + openbare_dienst + toeslagen + certificaten + accijnzen

    def get_distribution_price_fixed(self):
        databeheer = 18.921
        return databeheer / 12.0

    def get_eneryfund_price(self):
        return 0
