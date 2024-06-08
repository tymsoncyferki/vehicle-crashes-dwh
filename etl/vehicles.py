import re

import numpy as np
import pandas as pd

from utils import fnv1a_hash_16_digit
from utils import Static, Config


def prepare_vehicles_data(data):
    data = data[['id', 'make', 'baseModel', 'model', 'year', 'VClass', 'cylinders', 'displ', 'trany', 'drive',
                 'fuelType1', 'city08', 'highway08']]
    data.columns = ['VehicleKey', 'Make', 'BaseModel', 'Model', 'Year', 'BodyClass', 'Cylinders', 'Displacement',
                    'Transmission', 'Drivetrain', 'FuelType', 'CityMPG', 'HighwayMPG']
    data.astype(str)
    data.loc[:, ['VehicleKey', 'Year', 'Cylinders', 'Displacement', 'CityMPG', 'HighwayMPG']] = data[
        ['VehicleKey', 'Year', 'Cylinders', 'Displacement', 'CityMPG', 'HighwayMPG']].astype(float)
    return data.sort_values('VehicleKey')


def handle_nans_vehicles(data):
    data.loc[:, ['Cylinders', 'Displacement']] = data[['Cylinders', 'Displacement']].fillna(0)
    columns_to_fill = ['Make', 'BaseModel', 'Model', 'Year', 'BodyClass', 'Transmission', 'Drivetrain', 'FuelType']
    for col in columns_to_fill:
        data[col] = data[col].fillna('Unknown')
    return data


def transform_transmission(trans):
    if trans == 'Unknown':
        return trans
    try:
        gears = re.findall(r'\d+', trans)[0]
    except (Exception,):
        gears = 'CVT'
    if 'Automatic' in trans:
        return f"Automatic {gears}"
    elif 'Manual' in trans:
        return f"Manual {gears}"
    else:
        return trans


def transform_drivetrain(drive):
    if drive == "Front-Wheel Drive":
        return 'FWD'
    elif drive == "Rear-Wheel Drive":
        return "RWD"
    elif drive in ["4-Wheel or All-Wheel Drive", "All-Wheel Drive"]:
        return "AWD"
    elif drive in ["4-Wheel Drive", "Part-time 4-Wheel Drive"]:
        return "4WD"
    elif drive == '2-Wheel Drive':
        return "2WD"
    else:
        return drive


def generate_blank_models(data):
    data = data.copy()

    if Config.DWH_INITIALIZATION:
        makes = list(data['Make'].unique()) + list(Static.BRANDS_DICT.values())
    else:
        makes = list(data['Make'].unique())

    makes_unique = np.unique(makes)
    for make in makes_unique:
        row = {'Make': make,
               'Year': 0,
               'BaseModel': "Unknown",
               'VehicleKey': 'Unknown',
               'BodyClass': 'Unknown',
               'Cylinders': 0,
               'Displacement': 0,
               'Transmission': 'Unknown',
               'Drivetrain': 'Unknown',
               'FuelType': 'Unknown',
               'CityMPG': 0,
               'HighwayMPG': 0}
        data = pd.concat([data, pd.DataFrame([row])], ignore_index=True)

    return data


def aggregate_models(data):
    return data.drop(['Model'], axis=1).groupby(['Make', 'Year', 'BaseModel']).agg(
        lambda x: x.mode().iloc[0]).reset_index()


def generate_vehicle_key(make, model, year):
    make = make.replace(' ', '')
    model = model.replace(' ', '')
    return fnv1a_hash_16_digit(f"{make}{model}{year}")


def transform_vehicle_data(data):
    data['Transmission'] = data['Transmission'].apply(transform_transmission)
    data['Drivetrain'] = data['Drivetrain'].apply(transform_drivetrain)
    # generate blank objects for each brand
    data = generate_blank_models(data)
    # aggregate models
    data = aggregate_models(data)
    # generate keys
    data['VehicleKey'] = data.apply(lambda x: generate_vehicle_key(x.Make, x.BaseModel, x.Year), axis=1)
    return data


def vehicles_pipeline(raw_data):
    vehicles_prep = prepare_vehicles_data(raw_data)
    vehicles_nan = handle_nans_vehicles(vehicles_prep)
    vehicles = transform_vehicle_data(vehicles_nan)
    return vehicles
