import os

from dotenv import load_dotenv
from sodapy import Socrata
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads as load_wkt

load_dotenv('../.env')


class Config:

    DWH_INITIALIZATION = True
    """ if this is the first time laoding data into data warehouse """

    FROM_FILES = False
    """ if pipeline is run from local files instead api """


def load_brands_dict(return_=False):
    cars_mapper = pd.read_csv("../data/static/car_makes.txt")
    brands_dict = cars_mapper.set_index('unique_makes_to_map')['unique_makes'].to_dict()
    print('Brands mapper loaded')
    if return_:
        return brands_dict
    else:
        Static.BRANDS_DICT = brands_dict


def load_models_dict(return_=False):
    models_mapper = pd.read_csv("../data/static/car_models.csv")
    models_dict = {}

    for index, row in models_mapper.iterrows():
        key = (row['Year'], row['Make'])
        if key not in models_dict:
            models_dict[key] = []
        models_dict[key].append(row['BaseModel'])

    print('Models mapper loaded')

    if return_:
        return models_dict
    else:
        Static.MODELS_DICT = models_dict


def update_models_mapper(vehicles_agg):
    models_mapper = pd.read_csv("../data/static/car_models.csv")
    new_model_mapper = pd.concat([models_mapper, vehicles_agg])
    new_model_mapper = new_model_mapper.drop_duplicates()
    new_model_mapper.to_csv("../data/static/car_models.csv", index=False)
    print('Models mapper updated')


def load_area_mapper(return_=False):
    area_mapper = pd.read_csv("../data/area_mapper.csv")
    area_mapper['Geometry'] = area_mapper['Geometry'].apply(load_wkt)
    gdf = gpd.GeoDataFrame(area_mapper, geometry='Geometry')
    print("Area mapper loaded")
    if return_:
        return gdf
    else:
        Static.AREA_MAPPER = gdf


def load_zipcodes(return_=False):
    zipcodes = pd.read_csv("../data/ZIPCODES.csv")
    print('Zipcodes data loaded')
    if return_:
        return zipcodes
    else:
        Static.ZIPCODES = zipcodes


class Static:

    BRANDS_DICT = load_brands_dict(return_=True)
    """ mapper for car brands """

    MODELS_DICT = load_models_dict(return_=True)
    """ dictionary with keys 'Make', 'Year' and lists with 'BaseModel' as values for mapping car models """

    AREA_MAPPER = load_area_mapper(return_=True)
    """ mapper for mapping coordinates to location area key """

    ZIPCODES = load_zipcodes(return_=True)
    """ zipcodes data """


def soda_montgomery_request(dataset, start_date, end_date):
    """
    Fetch data from montgomery county data portal for given dataset and date interval

    Args:
        dataset (str): type of dataset to pull, available options: 'incidents', 'drivers', 'non-motorists'
        start_date (str): The start date for data retrieval in "YYYY-MM-DD" format.
        end_date (str): The end date for the data retrieval in "YYYY-MM-DD" format.

    Returns:
        DataFrame: A DataFrame containing
    """
    dataset_keys = {'incidents': 'bhju-22kf',
                    'drivers': 'mmzv-x632',
                    'non-motorists': 'n7fk-dce5'}
    data_key = dataset_keys[dataset]

    client = Socrata("data.montgomerycountymd.gov",
                     os.getenv('SOTA_TOKEN'),
                     username=os.getenv('SOTA_USER'),
                     password=os.getenv('SOTA_PWD'))

    start_date = start_date.split(' ')[0]
    end_date = end_date.split(' ')[0]
    where_clause = f"crash_date_time >= '{start_date}' AND crash_date_time <= '{end_date}'"

    results = client.get(data_key, where=where_clause, limit=1000000)
    results_df = pd.DataFrame.from_records(results)

    return results_df


def fnv1a_hash_16_digit(s: str) -> int:
    """
    FNV-1a Hash Function to hash a string to a 16-digit deterministic integer value.

    :param s: Input string to hash
    :return: Deterministic 16-digit integer hash value
    """
    fnv_prime = 0x1000193
    hash_value = 0xcbf29ce484222325

    for char in s:
        hash_value ^= ord(char)
        hash_value *= fnv_prime
        hash_value &= 0xffffffffffffffff

    return hash_value % 10 ** 16


def change_column_names(column_names):
    columns = [col.lower().replace('', '_') for col in column_names]
    return columns


