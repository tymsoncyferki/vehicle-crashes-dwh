import pandas as pd

from vehicles import vehicles_pipeline
from drivers import drivers_pipeline, drivers_mapping_pipeline
from crashes import crashes_pipeline, mapping_pipeline
from nonmotorists import nonmoto_pipeline
from roads import road_pipeline
from utils import load_brands_dict, load_models_dict, update_models_mapper, load_area_mapper, soda_montgomery_request


class ETL:

    def __init__(self):
        self.crash_data = None
        self.weather_data = None
        self.vehicles_data = None
        self.drivers_data = None
        self.road_data = None
        self.nonmotorists_data = None

    def extract_data(self, start_date, end_date):
        """ load data from different sources """
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
        # filter out vehicles already in dwh
        print('vehicles data rows:', len(self.vehicles_data))

        # self.crash_data = pd.read_csv("../data/montgomery_incidents_data.csv")
        # self.drivers_data = pd.read_csv("../data/montgomery_drivers.csv")
        # self.nonmotorists_data = pd.read_csv("../data/montgomery_nonmotorist.csv")
        # self.vehicles_data = pd.read_csv("../data/vehicles.csv")

    def transform_data(self):
        """ run transformations """
        print('RUNNING TRANSFORMATIONS')

        load_brands_dict()
        print('Brands mapper loaded')
        self.vehicles_data = vehicles_pipeline(self.vehicles_data)
        print('Vehicles data transformed')

        update_models_mapper(self.vehicles_data[['Make', 'Year', 'BaseModel']])
        print('Models mapper updated')
        load_models_dict()  # needs to be loaded after vehicles pipeline and updating mapper
        print('Models mapper loaded')

        self.drivers_data = drivers_pipeline(self.drivers_data)
        print('Drivers data transformed')

        # crash data is still raw
        self.road_data = road_pipeline(self.crash_data)
        print('Roads data transformed')

        self.nonmotorists_data = nonmoto_pipeline(self.nonmotorists_data)
        print('Non-motorists data transformed')

        self.crash_data = crashes_pipeline(self.crash_data)
        print('Crashes data transformed')

    def join_data(self):
        """ generate foreign keys """
        print('RUNNING JOINING')

        load_area_mapper()
        print("Area mapper loaded")

        self.drivers_data = drivers_mapping_pipeline(self.drivers_data)
        print('Drivers data mapped')

        self.crash_data = mapping_pipeline(self.crash_data, self.nonmotorists_data)
        print('Crashes data mapped')

        self.drivers_data = self.drivers_data.merge(self.crash_data, on='ReportNumber')
        print('Vehicle Crashes joined')

    def load_data(self):
        """ load data to dwh"""
        print('Vehicle Crashes table rows:', len(self.drivers_data))

        self.drivers_data.to_csv("../data/etl_out/VehicleCrashFact.csv", index=False)
        self.vehicles_data.to_csv("../data/etl_out/VehicleDim.csv", index=False)
        self.road_data.to_csv("../data/etl_out/RoadDim.csv", index=False)

        print('Tables saved succesfully')


def main():
    etl = ETL()
    etl.extract_data('2023-01-01', '2023-12-31')
    etl.transform_data()
    etl.join_data()
    etl.load_data()


if __name__ == "__main__":
    main()
