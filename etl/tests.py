import unittest

from utils import soda_montgomery_request, Static
from location import generate_location_area_dim
from weather import extract_weather_data, transform_weather_fact
from datehour import generate_date_hour_dim


class TestUtils(unittest.TestCase):

    def test_soda_request(self):
        # start_date = "2015-01-01"
        # end_date = "2023-12-31"
        # df = soda_montgomery_request('drivers', start_date, end_date)

        df = soda_montgomery_request('incidents', start_date='2023-12-01', end_date='2023-12-31')
        print(len(df))
        print(df.columns)
        self.assertGreater(len(df), 900)


class TestLocation(unittest.TestCase):

    def test_location_generation(self):
        df = generate_location_area_dim(Static.ZIPCODES)
        self.assertEqual(len(df), 98)


class TestWeather(unittest.TestCase):

    def test_weatherkey_generation(self):
        df_raw = extract_weather_data(Static.ZIPCODES, '2023-12-01 00:00:00', '2023-12-31 23:00:00')
        df = transform_weather_fact(df_raw)
        nulls = len(df[df.isna().any(axis=1)])
        self.assertEqual(nulls, 0)
        print(df.head(5))
        print(df.tail(5))


class TestDateHour(unittest.TestCase):

    def test_datehour_generation(self):
        df = generate_date_hour_dim()
        nulls = len(df[df.isna().any(axis=1)])
        self.assertEqual(nulls, 0)