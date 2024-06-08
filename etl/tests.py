import unittest

from utils import soda_montgomery_request, Static
from location import generate_location_area_dim
from weather import extract_weather_data


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

    pass