from datetime import datetime
from difflib import get_close_matches

import pandas as pd

from utils import Static
from vehicles import generate_vehicle_key


def change_to_unknown(string):
    return 'UNKNOWN' if (string.lower() == 'unknown' or string.lower() == 'nan' or string.lower() == 'n/a' or string == '') else string


def clean_substance_abuse(substance):
    """ cleans substance abuse column keeping only substance names """
    substance = substance.lower().replace('present', '').replace('contributed', '').replace('detected', '').strip()
    if 'combin' in substance:
        return 'COMBINATION'
    else:
        return substance.upper()


def map_vehicle_type(vehicle):
    """ maps vehicle types to more general ones """
    vehicle = vehicle.lower()

    def in_vehicle(types):
        return any([car_type in vehicle for car_type in types])

    # passenger car
    if in_vehicle(['passenger', 'utility', 'pickup', 'van', 'wagon', 'limousine']) and 'over' not in vehicle:
        return 'PASSENGER'
    elif in_vehicle(['emergency']):
        return 'EMERGENCY'
    elif in_vehicle(['motorcycle', 'moped']):
        return 'MOTORCYCLE'
    elif in_vehicle(['bus']):
        return 'BUS'
    elif in_vehicle(['truck']):
        return 'TRUCK'
    elif in_vehicle(['unknown']):
        return 'UNKNOWN'
    else:
        return 'OTHER'


def prepare_data(data):
    """ selects columns, renames them and changes types """
    data = data[['report_number', 'vehicle_id', 'driver_at_fault', 'injury_severity', 'driver_substance_abuse',
                 'driver_distracted_by', 'vehicle_body_type', 'vehicle_movement',
                 'vehicle_going_dir', 'vehicle_damage_extent', 'speed_limit', 'parked_vehicle', 'vehicle_year',
                 'vehicle_make', 'vehicle_model']]

    data.columns = ['ReportNumber', 'VehicleCrashKey', 'DriverAtFault', 'DriverInjurySeverity', 'DriverSubstanceAbuse',
                    'DriverDistractedBy', 'VehicleType', 'VehicleMovement',
                    'VehicleGoingDir', 'VehicleDamageExtent', 'SpeedLimit', 'ParkedVehicle', 'VehicleYear',
                    'VehicleMake', 'VehicleModel']
    data = data.astype(str)
    data['VehicleYear'] = data['VehicleYear'].astype(int)
    return data


def handle_nans(data):
    """ handles na values """
    data = data.copy()
    data['SpeedLimit'] = data['SpeedLimit'].fillna(0)
    data['VehicleYear'] = data['VehicleYear'].fillna(0)
    # str columns changed to unknown
    columns_to_unknown = ['DriverSubstanceAbuse', 'DriverDistractedBy', 'VehicleType', 'VehicleMovement',
                          'VehicleGoingDir',
                          'VehicleDamageExtent', 'VehicleMake', 'VehicleModel']
    for col in columns_to_unknown:
        data[col] = data[col].apply(change_to_unknown)
    return data


def transform_columns(data):
    """ performs neccessary transformations """
    data = data.copy()
    # clean primary key
    data['VehicleCrashKey'] = data['VehicleCrashKey'].apply(lambda x: x.replace('-', ''))
    # boolean if driver at fault
    data['DriverAtFault'] = data['DriverAtFault'].apply(lambda x: True if x == 'Yes' else False)
    # boolean if substance contributed
    data['SubstanceAbuseContributed'] = data['DriverSubstanceAbuse'].apply(
        lambda x: True if 'contributed' in x.lower() else False)
    # clean substance
    data['DriverSubstanceAbuse'] = data['DriverSubstanceAbuse'].apply(clean_substance_abuse)
    # map vehicle types
    data['VehicleType'] = data['VehicleType'].apply(map_vehicle_type)
    # boolean
    data['ParkedVehicle'] = data['ParkedVehicle'].apply(lambda x: True if x == 'Yes' else False)
    # delete impossible year values
    data['VehicleYear'] = data['VehicleYear'].apply(lambda x: 0 if (x < 1900 or x > (datetime.now().year + 1)) else x)

    # calculate vehicles crashed total
    crashed_total = data.groupby('ReportNumber').agg(
        VehiclesCrashedTotal=pd.NamedAgg('ReportNumber', 'count')).reset_index()
    data = data.merge(crashed_total, on='ReportNumber')
    return data


def drivers_pipeline(raw_data):
    drivers_prep = prepare_data(raw_data)
    drivers_nonan = handle_nans(drivers_prep)
    drivers_safe = transform_columns(drivers_nonan)
    return drivers_safe


def map_makes(make):
    """ maps crash data brands to fueleconomy car brands"""
    try:
        new_make = Static.BRANDS_DICT[make]
    except KeyError:
        new_make = 'No match found'

    if new_make == 'No match found':
        makes_lower = [m.lower() for m in list(Static.BRANDS_DICT.values())]
        found_makes = get_close_matches(make.lower(), makes_lower, n=1, cutoff=0.5)

        if len(found_makes) == 0:
            new_make = 'Unknown'
        else:
            found_make_lower = found_makes[0]
            found_make_original = list(Static.BRANDS_DICT.values())[makes_lower.index(found_make_lower)]
            return found_make_original

    return new_make


def map_models(model, make, year):
    """ maps crash data models to fueleconomy car models """
    if model.lower() in ['4s', 'tk']:
        return 'Unknown'

    try:
        models_raw = list(set(Static.MODELS_DICT[(year, make)]))
        models_lower = [m.lower() for m in models_raw]
        found_models_lower = get_close_matches(model.lower(), models_lower, n=1, cutoff=0.2)
    except KeyError:
        return 'Unknown'

    if len(found_models_lower) == 0:
        return 'Unknown'

    found_model_lower = found_models_lower[0]
    found_model_original = models_raw[models_lower.index(found_model_lower)]
    return found_model_original


def map_year(year, model):
    if model == 'Unknown':
        return 0
    else:
        return year


def drivers_mapping_pipeline(data):
    data = data.copy()
    data['MappedMake'] = data['VehicleMake'].apply(map_makes)
    data['MappedModel'] = data.apply(lambda x: map_models(x.VehicleModel, x.MappedMake, x.VehicleYear), axis=1)
    data['MappedYear'] = data.apply(lambda x: map_year(x.VehicleYear, x.MappedModel), axis=1)
    data['VehicleKey'] = data.apply(lambda x: generate_vehicle_key(x.MappedMake, x.MappedModel, x.MappedYear), axis=1)
    data = data.drop(['VehicleYear', 'VehicleMake', 'VehicleModel', 'MappedMake', 'MappedModel', 'MappedYear'], axis=1)
    return data
