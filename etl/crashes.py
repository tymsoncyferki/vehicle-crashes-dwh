from datetime import datetime

import pandas as pd
from shapely.geometry import Point

from roads import generate_roaddim_key
from utils import Static, Config


def filter_columns(data):
    """ keeps only necessary columns with changed names and format """
    # keep columns
    data = data[['report_number', 'local_case_number', 'agency_name', 'acrs_report_type', 'crash_date_time', 'hit_run',
                 'route_type', 'lane_direction', 'lane_number',
                 'number_of_lanes', 'road_grade', 'nontraffic', 'road_name', 'cross_street_type', 'cross_street_name',
                 'off_road_description',
                 'at_fault', 'collision_type', 'surface_condition', 'light', 'traffic_control', 'junction',
                 'intersection_type',
                 'road_alignment', 'road_condition', 'road_division', 'latitude', 'longitude']]

    # change names
    data.columns = ['ReportNumber', 'LocalCaseNumber', 'AgencyName', 'ACRSReportType', 'Datetime', 'HitRun',
                    'RouteType', 'LaneDirection', 'LaneNumber',
                    'NumberOfLanes', 'RoadGrade', 'NonTraffic', 'RoadName', 'CrossStreetType', 'CrossStreetName',
                    'OffRoadIncident',
                    'AccidentAtFault', 'CollisionType', 'SurfaceCondition', 'Light', 'TrafficControl', 'Junction',
                    'IntersectionType',
                    'RoadAlignment', 'RoadCondition', 'RoadDivision', 'Latitude', 'Longitude']
    # change format
    data = data.astype(str)
    data['LaneNumber'] = pd.to_numeric(data['LaneNumber'])
    data['NumberOfLanes'] = pd.to_numeric(data['NumberOfLanes'])
    data['Latitude'] = pd.to_numeric(data['Latitude'])
    data['Longitude'] = pd.to_numeric(data['Longitude'])
    return data


def change_to_unknown(string):
    return 'UNKNOWN' if (string.lower() == 'unknown' or string == 'nan' or string == '') else string


def handle_nans(data):
    data = data.copy()
    #
    data['LaneNumber'] = data['LaneNumber'].fillna(0)
    data['NumberOfLanes'] = data['NumberOfLanes'].fillna(0)
    # str columns changed to unknown
    columns_to_unknown = ['AgencyName', 'ACRSReportType', 'RouteType', 'LaneDirection', 'RoadGrade', 'RoadName',
                          'CrossStreetType', 'CrossStreetName', 'AccidentAtFault',
                          'CollisionType', 'SurfaceCondition', 'Light', 'TrafficControl', 'Junction',
                          'IntersectionType', 'RoadAlignment', 'RoadCondition', 'RoadDivision']
    for col in columns_to_unknown:
        data[col] = data[col].apply(change_to_unknown)
    # Datetime, HitRun, NonTraffic, OffRoadIncident handled in transform
    return data


def map_to_datetime(date_str):
    try:
        if Config.FROM_FILES:
            date = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
        else:
            date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
    except (Exception,):
        date = ""
    return date


def transform_columns(data):
    data = data.copy()
    # nothing to clean 'ReportNumber', 'LocalCaseNumber', 'AgencyName', maybe validate?
    # clean acrs report type
    data['ACRSReportType'] = data['ACRSReportType'].apply(lambda x: x.replace("Crash", ""))
    # crash date to datetime format
    data['Datetime'] = data['Datetime'].apply(map_to_datetime)
    # change hitrun to boolean
    data['HitRun'] = data['HitRun'].apply(lambda x: True if x == 'Yes' else False)
    data['NonTraffic'] = data['NonTraffic'].apply(lambda x: True if x == 'Yes' else False)
    # map offroadincident to binary
    data['OffRoadIncident'] = data['OffRoadIncident'].apply(lambda x: False if (x == 'nan' or x == '') else True)
    # obrobienie ładnie collision type?
    return data


def crashes_pipeline(crashes_raw):
    crashes_filtered = filter_columns(crashes_raw)
    crashes_nonull = handle_nans(crashes_filtered)
    crashes = transform_columns(crashes_nonull)
    return crashes


def generate_date_hour_dim_key(data):
    return data['Datetime'].dt.strftime('%Y%m%d%H').astype(int)


def map_location(lat, long, gdf):
    point = Point(float(long), float(lat))
    result = gdf[gdf['Geometry'].contains(point)].reset_index()
    try:
        area_key = result.loc[0]['LocationAreaKey']
    except (Exception,):
        area_key = 0
    return area_key


def mapping_pipeline(crashes, nonmoto_agg):
    # Add RoadKey and CrossStreetKey
    crashes_joined = crashes.copy()
    crashes_joined['RoadKey'] = crashes.apply(lambda x: generate_roaddim_key(x.RoadName, x.RouteType), axis=1)
    crashes_joined['CrossStreetKey'] = crashes.apply(
        lambda x: generate_roaddim_key(x.CrossStreetName, x.CrossStreetType), axis=1)
    crashes_joined.drop(['RoadName', 'RouteType', 'CrossStreetName', 'CrossStreetType'], axis=1, inplace=True)

    # Add non motorists aggregated measures
    crashes_nonmoto = crashes_joined.merge(nonmoto_agg, how='left', on='ReportNumber')
    crashes_nonmoto[["NonMotoristTotal", "NonMotoristInjury", "NonMotoristFatal"]] = crashes_nonmoto[
        ["NonMotoristTotal", "NonMotoristInjury", "NonMotoristFatal"]].fillna(0).astype(int)

    # Add DateHourKey
    crashes_nonmoto['DateHourKey'] = generate_date_hour_dim_key(crashes_nonmoto)
    crashes_nonmoto.drop('Datetime', inplace=True, axis=1)

    # Add LocationAreaKey
    crashes_nonmoto['LocationAreaKey'] = crashes_nonmoto.apply(
        lambda x: map_location(x.Latitude, x.Longitude, Static.AREA_MAPPER), axis=1)

    return crashes_nonmoto
