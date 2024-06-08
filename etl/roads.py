import pandas as pd

from utils import fnv1a_hash_16_digit


def change_to_unknown(string):
    return 'UNKNOWN' if (string.lower() == 'unknown' or string == 'nan' or string.lower() == 'n/a' or string == '') else string


def prepare_roaddim_data(data):
    data = data[['road_name', 'route_type', 'cross_street_name', 'cross_street_type']]
    data.columns = ['RoadName', 'RouteType', 'CrossStreetName', 'CrossStreetType']
    data = data.astype(str)
    for col in data.columns:
        data[col] = data[col].apply(change_to_unknown)
    return data


def generate_roaddim_key(name_col, type_col):
    unique_str = name_col.replace(' ', '_') + type_col.split()[0]
    return fnv1a_hash_16_digit(unique_str)


def transform_road_data(data):
    roads = data[['RoadName', 'RouteType']]
    crossroads = data[['CrossStreetName', 'CrossStreetType']]
    crossroads.columns = ['RoadName', 'RouteType']
    road_dim = pd.concat([roads, crossroads]).drop_duplicates()
    road_dim['RoadKey'] = road_dim.apply(lambda x: generate_roaddim_key(x.RoadName, x.RouteType), axis=1)
    return road_dim


def road_pipeline(crashes_raw):
    crashes_road = prepare_roaddim_data(crashes_raw)
    road_dim = transform_road_data(crashes_road)
    return road_dim
