import pandas as pd

from vehicles import vehicles_pipeline
from drivers import drivers_pipeline, drivers_mapping_pipeline
from crashes import crashes_pipeline, mapping_pipeline
from nonmotorists import nonmoto_pipeline
from roads import road_pipeline
from weather import extract_weather_data, transform_weather_fact
from datehour import generate_date_hour_dim
from location import generate_location_area_dim
from insertion import load_data_to_dwh
from utils import load_models_dict, update_models_mapper, soda_montgomery_request, Static
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
        print(f'RUNNING EXTRACTION from {start_date} to {end_date}')

        crash_data = soda_montgomery_request('incidents', start_date=start_date, end_date=end_date)
        self.crash_data = crash_data
        print('crash data rows:', len(self.crash_data))

        drivers_data = soda_montgomery_request('drivers', start_date=start_date, end_date=end_date)
        self.drivers_data = drivers_data
        print('drivers data rows:', len(self.drivers_data))

        nonmotorists_data = soda_montgomery_request('non-motorists', start_date=start_date, end_date=end_date)
        self.nonmotorists_data = nonmotorists_data
        print('non motorists data rows:', len(self.nonmotorists_data))

        vehicles_data = pd.read_csv("https://www.fueleconomy.gov/feg/epadata/vehicles.csv")
        self.vehicles_data = vehicles_data
        print('vehicles data rows:', len(self.vehicles_data))

        weather_data = extract_weather_data(Static.ZIPCODES, start_date=start_date, end_date=end_date)
        self.weather_data = weather_data
        print('weather data rows:', len(self.weather_data))

        self.datehour_data = generate_date_hour_dim(start_date=start_date, end_date=end_date)
        print('datehour data rows:', len(self.weather_data))

        # self.crash_data = pd.read_csv("../data/montgomery_incidents_data.csv")
        # self.drivers_data = pd.read_csv("../data/montgomery_drivers.csv")
        # self.nonmotorists_data = pd.read_csv("../data/montgomery_nonmotorist.csv")
        # self.vehicles_data = pd.read_csv("../data/vehicles.csv")

    def transform_data(self):
        """ run transformations """
        print("-----")
        print('RUNNING TRANSFORMATIONS')

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
        print('RUNNING JOINING')

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
        print("RUNNING DWH INSERTION")

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
            print('Tables inserted succesfully')


def main(start_date, end_date):
    print(f'RUNNING ETL PIPELINE from {start_date} to {end_date}')
    print("-----")
    etl = ETL()
    etl.extract_data(start_date, end_date)
    etl.transform_data()
    etl.join_data()
    if Config.DWH_INITIALIZATION:
        etl.merge_data()
    etl.load_data()
    green = '\033[92m'
    print(f"{green}ETL PROCESS FINISHED WITH SUCCESS{green}")


if __name__ == "__main__":
    main('2021-01-01 00:00:00', '2021-12-31 23:00:00')
