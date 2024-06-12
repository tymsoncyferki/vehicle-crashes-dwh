from datetime import datetime, timedelta
from dateutil.relativedelta import *

import pandas as pd

from vehicles import vehicles_pipeline
from drivers import drivers_pipeline, drivers_mapping_pipeline
from crashes import crashes_pipeline, mapping_pipeline
from nonmotorists import nonmoto_pipeline
from roads import road_pipeline
from weather import extract_weather_data, transform_weather_fact
from datehour import generate_date_hour_dim
from location import generate_location_area_dim
from insertion import load_data_to_dwh, check_last_update
from utils import load_models_dict, update_models_mapper, soda_montgomery_request, Static, Colors
from config import Config


class ETL:

    def __init__(self):
        self.crash_data = pd.DataFrame()
        self.drivers_data = pd.DataFrame()
        self.nonmotorists_data = pd.DataFrame()
        self.vehicles_data = pd.DataFrame()
        self.road_data = pd.DataFrame()
        self.weather_data = pd.DataFrame()
        self.datehour_data = pd.DataFrame()
        self.location_data = pd.DataFrame()
        self.merged_data = pd.DataFrame()

    def extract_data(self, start_date, end_date):
        """ load data from different sources """
        print("-----")
        print(f'{Colors.PURPLE}RUNNING EXTRACTION from {start_date} to {end_date}{Colors.RESET}')

        crash_data = soda_montgomery_request('incidents', start_date=start_date, end_date=end_date)
        self.crash_data = crash_data
        print('crash data rows:', len(self.crash_data))

        drivers_data = soda_montgomery_request('drivers', start_date=start_date, end_date=end_date)
        self.drivers_data = drivers_data
        print('drivers data rows:', len(self.drivers_data))

        nonmotorists_data = soda_montgomery_request('non-motorists', start_date=start_date, end_date=end_date)
        self.nonmotorists_data = nonmotorists_data
        print('non motorists data rows:', len(self.nonmotorists_data))

        vehicles_data = pd.read_csv("https://www.fueleconomy.gov/feg/epadata/vehicles.csv", low_memory=False)
        self.vehicles_data = vehicles_data
        print('vehicles data rows:', len(self.vehicles_data))

        weather_data = extract_weather_data(Static.ZIPCODES, start_date=start_date, end_date=end_date)
        self.weather_data = weather_data
        print('weather data rows:', len(self.weather_data))

        self.datehour_data = generate_date_hour_dim(start_date=start_date, end_date=end_date)
        print('datehour data rows:', len(self.datehour_data))

    def transform_data(self):
        """ run transformations """
        print("-----")
        print(f'{Colors.PURPLE}RUNNING TRANSFORMATIONS{Colors.RESET}')

        self.vehicles_data = vehicles_pipeline(self.vehicles_data)
        print('Vehicles data transformed')

        update_models_mapper(self.vehicles_data[['Make', 'Year', 'BaseModel']])
        load_models_dict()  # needs to be loaded after vehicles pipeline and updating mapper

        self.drivers_data = drivers_pipeline(self.drivers_data)
        print('Drivers data transformed')

        # crash data is still raw
        self.road_data = road_pipeline(self.crash_data)
        print('Roads data transformed')

        self.nonmotorists_data = nonmoto_pipeline(self.nonmotorists_data)
        print('Non-motorists data transformed')

        self.crash_data = crashes_pipeline(self.crash_data)
        print('Crashes data transformed')

        self.weather_data = transform_weather_fact(self.weather_data)
        print('Weather data transformed')

        if Config.DWH_INITIALIZATION:
            self.location_data = generate_location_area_dim(Static.ZIPCODES)
            print('Location data generated')

    def join_data(self):
        """ generate foreign keys """
        print("-----")
        print(f'{Colors.PURPLE}RUNNING JOINING{Colors.RESET}')

        self.drivers_data = drivers_mapping_pipeline(self.drivers_data)
        print('Drivers data mapped')

        self.crash_data = mapping_pipeline(self.crash_data, self.nonmotorists_data)
        print('Crashes data mapped')

        self.drivers_data = self.drivers_data.merge(self.crash_data, on='ReportNumber')
        print('Vehicle Crashes joined')

    def merge_data(self):
        """ merges all data for testing purposes """
        try:
            self.merged_data = self.drivers_data.merge(
                self.vehicles_data, on='VehicleKey').merge(
                self.road_data, left_on='RoadKey', right_on='RoadKey').merge(
                self.road_data, left_on='CrossStreetKey', right_on='RoadKey').merge(
                self.location_data, on='LocationAreaKey').merge(
                self.weather_data, on=['LocationAreaKey', 'DateHourKey']).merge(
                self.datehour_data, on='DateHourKey'
            )
        except (Exception, ) as e:
            print(e)
        fact_rows = len(self.drivers_data)
        merged_rows = len(self.merged_data)
        print('Vehicle Crashes table rows:', fact_rows)
        print('Merged table rows:', merged_rows)
        if fact_rows > merged_rows:
            print('WARNING: Data cannot be fully merged')

    def load_data(self):
        """ load data to dwh"""
        print("-----")
        print(f"{Colors.PURPLE}RUNNING DWH INSERTION{Colors.RESET}")

        if Config.DEBUG:
            self.drivers_data.to_csv("out/VehicleCrashFact.csv", index=False)
            self.vehicles_data.to_csv("out/VehicleDim.csv", index=False)
            self.road_data.to_csv("out/RoadDim.csv", index=False)
            self.weather_data.to_csv("out/WeatherFact.csv", index=False)
            self.datehour_data.to_csv("out/DateHourDim.csv", index=False)
            if Config.DWH_INITIALIZATION:
                self.location_data.to_csv("out/LocationAreaDim.csv", index=False)
            self.merged_data.to_csv("out/MergedData.csv", index=False)
            print('Tables saved succesfully')

        else:
            load_data_to_dwh(self.road_data, 'RoadDim')
            load_data_to_dwh(self.vehicles_data, 'VehicleDim')
            if Config.DWH_INITIALIZATION:
                load_data_to_dwh(self.location_data, 'LocationAreaDim')
            load_data_to_dwh(self.datehour_data, 'DateHourDim')
            load_data_to_dwh(self.weather_data, 'WeatherFact')
            load_data_to_dwh(self.drivers_data, 'VehicleCrashFact')


def etl_pipeline(start_date=None, end_date=None, message=None):
    """ runs ETL pipeline """

    try:
        if start_date is None or end_date is None:
            print("-----")
            print("Checking last update date...")
            last_end_date = check_last_update()
            start_date = last_end_date + timedelta(hours=1)
            end_date = (start_date + relativedelta(months=1) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")
            if message is None:
                message = 'regular update'
        else:
            if message is None:
                message = 'custom update'
    except (Exception, ) as e:
        print("Error ocurred during last update check, aborting...", e)
        return

    print("-----")
    print(f'{Colors.PURPLE}RUNNING ETL PIPELINE from {start_date} to {end_date}{Colors.RESET}')

    etl = ETL()

    try:
        etl.extract_data(start_date, end_date)
    except (Exception, ) as e:
        print("Error ocurred during extraction phase, aborting...", e)
        return

    try:
        etl.transform_data()
    except (Exception, ) as e:
        print("Error ocurred during transform phase, aborting...", e)
        return

    try:
        etl.join_data()
    except (Exception, ) as e:
        print("Error ocurred during joining phase, aborting...", e)
        return

    # otherwise it cannot be merged (no access to all location and vehicles data)
    if Config.DWH_INITIALIZATION:
        try:
            etl.merge_data()
        except (Exception,):
            pass

    try:
        etl.load_data()

        if not Config.DEBUG:
            update_data = pd.DataFrame({
                'LastUpdate': datetime.now(),
                'StartDate': start_date,
                'EndDate': end_date,
                'UpdateMessage': message
            }, index=[0])
            load_data_to_dwh(update_data, 'Metadata', skip_duplicates=False)

    except (Exception, ) as e:
        print("Error ocurred during load phase, aborting...", e)
        return

    print(f"{Colors.GREEN}ETL PROCESS FINISHED WITH SUCCESS{Colors.RESET}")


if __name__ == "__main__":
    # etl_pipeline('2015-08-01 00:00:00', '2015-08-31 23:00:00', message='custom update for 2 months')
    etl_pipeline()
