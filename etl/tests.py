import unittest

import pandas as pd

from utils import soda_montgomery_request, Static, fnv1a_hash_16_digit
from location import generate_location_area_dim
from weather import extract_weather_data, transform_weather_fact
from datehour import generate_date_hour_dim
from insertion import load_data_to_dwh, check_last_update
from crashes import crashes_pipeline, map_location
from drivers import map_models, map_makes, drivers_pipeline
from nonmotorists import nonmoto_pipeline
from roads import road_pipeline


class TestInsertion(unittest.TestCase):

    def test_insert_roaddim(self):
        roaddim = pd.read_csv("out/RoadDim.csv")
        success = load_data_to_dwh(roaddim, 'RoadDim')
        self.assertTrue(success)

    def test_check_update(self):
        end_date = check_last_update()
        print(end_date)


class TestUtils(unittest.TestCase):

    def test_soda_request_incidents(self):
        df = soda_montgomery_request('incidents', start_date='2023-12-01', end_date='2023-12-31')
        self.assertGreater(len(df), 900)

    def test_soda_request_drivers(self):
        df = soda_montgomery_request('drivers', start_date='2023-12-01', end_date='2023-12-31')
        self.assertGreater(len(df), 1500)

    def test_soda_request_nonmotorists(self):
        df = soda_montgomery_request('non-motorists', start_date='2023-12-01', end_date='2023-12-31')
        self.assertGreater(len(df), 30)

    def test_hash_function(self):
        hash1 = fnv1a_hash_16_digit("ANDERSONAVECounty")
        hash2 = fnv1a_hash_16_digit("ANDERSONAVECounty")
        self.assertEqual(hash1, hash2)
        self.assertEqual(type(hash1), int)
        self.assertEqual(len(str(hash1)), 16)


class TestLocation(unittest.TestCase):

    def test_location_generation(self):
        df = generate_location_area_dim(Static.ZIPCODES)
        self.assertEqual(len(df), 98)


class TestRoadDim(unittest.TestCase):

    def test_roaddim_pipeline(self):
        df_raw = soda_montgomery_request('incidents', start_date='2023-12-01', end_date='2023-12-31')
        df = road_pipeline(df_raw)
        nulls = len(df[df.isna().any(axis=1)])
        self.assertEqual(nulls, 0)


class TestCrashes(unittest.TestCase):

    def test_crashes_pipeline(self):
        df_raw = soda_montgomery_request('incidents', start_date='2023-12-01', end_date='2023-12-31')
        df = crashes_pipeline(df_raw)
        nulls = len(df[df.isna().any(axis=1)])
        self.assertEqual(nulls, 0)

    def test_location_mapping(self):
        key = map_location(39.077134, -77.146004, Static.AREA_MAPPER)
        self.assertGreater(key, 0)

    def test_location_mapping_unknown(self):
        key = map_location(69.077134, -27.146004, Static.AREA_MAPPER)
        self.assertEqual(key, 0)


class TestWeather(unittest.TestCase):

    def test_weather_generation(self):
        df_raw = extract_weather_data(Static.ZIPCODES, '2023-12-01 00:00:00', '2023-12-31 23:00:00')
        df = transform_weather_fact(df_raw)
        nulls = len(df[df.isna().any(axis=1)])
        self.assertEqual(nulls, 0)


class TestDrivers(unittest.TestCase):

    def test_make_mapping(self):
        make = map_makes('oYOTA')
        self.assertEqual("Toyota", make)

    def test_model_mapping(self):
        model = map_models('yrs', 'Toyota', 2015)
        self.assertEqual(model, 'Yaris')

    def test_model_mapping_unknown(self):
        model = map_models('X3', 'Toyota', 2015)
        self.assertEqual(model, 'Unknown')

    def test_drivers_pipeline(self):
        df_raw = soda_montgomery_request('drivers', start_date='2023-12-01', end_date='2023-12-31')
        df = drivers_pipeline(df_raw)
        nulls = len(df[df.isna().any(axis=1)])
        self.assertEqual(nulls, 0)


class TestNonMotorists(unittest.TestCase):

    def test_nonmoto_pipeline(self):
        df_raw = soda_montgomery_request('non-motorists', start_date='2023-12-01', end_date='2023-12-31')
        df = nonmoto_pipeline(df_raw)
        nulls = len(df[df.isna().any(axis=1)])
        self.assertEqual(nulls, 0)


class TestDateHour(unittest.TestCase):

    def test_datehour_generation(self):
        df = generate_date_hour_dim()
        nulls = len(df[df.isna().any(axis=1)])
        self.assertEqual(nulls, 0)
