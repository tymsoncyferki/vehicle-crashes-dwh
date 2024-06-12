from datetime import datetime

from sodapy import Socrata
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads as load_wkt

from config import Config


def load_brands_dict(return_=False):
    cars_mapper = pd.read_csv("static/car_makes.txt")
    brands_dict = cars_mapper.set_index('unique_makes_to_map')['unique_makes'].to_dict()
    print('Brands mapper loaded')
    if return_:
        return brands_dict
    else:
        Static.BRANDS_DICT = brands_dict


def load_models_dict(return_=False):
    models_mapper = pd.read_csv("static/car_models.csv")
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
    models_mapper = pd.read_csv("static/car_models.csv")
    new_model_mapper = pd.concat([models_mapper, vehicles_agg])
    new_model_mapper = new_model_mapper.drop_duplicates()
    new_model_mapper.to_csv("static/car_models.csv", index=False)
    print('Models mapper updated')


def load_area_mapper(return_=False):
    area_mapper = pd.read_csv("static/area_mapper.csv")
    area_mapper['Geometry'] = area_mapper['Geometry'].apply(load_wkt)
    gdf = gpd.GeoDataFrame(area_mapper, geometry='Geometry')
    print("Area mapper loaded")
    if return_:
        return gdf
    else:
        Static.AREA_MAPPER = gdf


def load_zipcodes(return_=False):
    zipcodes = pd.read_csv("static/ZIPCODES.csv")
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
                     Config.SOTA_TOKEN,
                     username=Config.SOTA_USER,
                     password=Config.SOTA_PWD)

    start_date = start_date.split(' ')[0]
    end_date = end_date.split(' ')[0]
    where_clause = f"crash_date_time >= '{start_date}' AND crash_date_time <= '{end_date}'"

    results_df = None

    if not Config.LOCAL_FILES:
        for _ in range(Config.N_RETRIES):
            try:
                results = client.get(data_key, where=where_clause, limit=1000000)
                results_df = pd.DataFrame.from_records(results)
                break
            except (Exception, ) as e:
                print("Error ocurred, sending another request", e)
                continue

    if results_df is None:
        # raise ConnectionError(f"Could not fetch {dataset} from montgomery data portal")
        print(f"{Colors.YELLOW}Could not connect to {dataset}-data-{data_key}@data.montgomerycountymd.gov,"
              f" loading local data{Colors.RESET}")
        results_df = load_local_montgomery_data(dataset, start_date, end_date)
    else:
        print(f"Connection to {data_key}@data.montgomerycountymd.gov succesful")

    return results_df


def load_local_montgomery_data(dataset, start_date, end_date):
    df = pd.read_csv(f"emergency/{dataset}.csv", low_memory=False)
    df.columns = change_column_names(df.columns)

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    def map_to_datetime(date_str):
        try:
            date = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
        except (Exception,):
            date = pd.NaT
        return date

    df['crash_custom'] = df['crash_date_time'].apply(map_to_datetime)
    filtered_df = df[(df['crash_custom'] >= start_date) & (df['crash_custom'] <= end_date)].copy()
    filtered_df.drop(['crash_custom'], axis=1, inplace=True)

    # crash_date_time
    return filtered_df


def fnv1a_hash_16_digit(s: str) -> int:
    """
    FNV-1a Hash Function to hash a string to a 16-digit deterministic integer value.

    Args:
        s (str): Input string to hash

    Returns:
        Deterministic 16-digit integer hash value
    """
    fnv_prime = 0x1000193
    hash_value = 0xcbf29ce484222325

    for char in s:
        hash_value ^= ord(char)
        hash_value *= fnv_prime
        hash_value &= 0xffffffffffffffff

    return hash_value % 10 ** 16


def change_column_names(column_names):
    columns = [col.lower().replace(' ', '_').replace('-', '_').replace('/', '_') for col in column_names]
    return columns


class Colors:
    PURPLE = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'
