import pandas as pd
import numpy as np
from shapely.wkt import loads as load_wkt
import geopandas as gpd
import openmeteo_requests
import requests_cache
from retry_requests import retry

from utils import Config


def extract_weather_data(zipcodes, start_date="2023-12-01 00:00:00", end_date="2023-12-31 23:00:00"):
    """
    Extract a weather DataFrame for given ZIP codes within a specified date range.

    Args:
        zipcodes (DataFrame): A DataFrame containing ZIP codes and their geometries.
        start_date (str): The start date for the weather data retrieval in "YYYY-MM-DD HH:MM:SS" format.
        end_date (str): The end date for the weather data retrieval in "YYYY-MM-DD HH:MM:SS" format.

    Returns:
        DataFrame: A DataFrame containing weather data for the specified ZIP codes and date range.
    """
    # Convert 'the_geom' to geometry and calculate centroids
    zipcodes['geometry'] = zipcodes['the_geom'].apply(load_wkt)
    gdf = gpd.GeoDataFrame(zipcodes, geometry='geometry')
    gdf['centroid'] = gdf['geometry'].centroid

    gdf['centroid_latitude'] = gdf['centroid'].y
    gdf['centroid_longitude'] = gdf['centroid'].x

    # Extract unique locations
    unique_locations = gdf[['ZIPCODE', 'centroid_latitude', 'centroid_longitude']].drop_duplicates()

    if Config.DWH_INITIALIZATION:
        unknown_location = {
            'ZIPCODE': 0,
            'centroid_latitude': 0,
            'centroid_longitude': 0
        }

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"

    # List to store all dataframes
    dfs = []

    # Iterate over DataFrame rows
    for _, row in unique_locations.iterrows():
        params = {
            "latitude": row['centroid_latitude'],
            "longitude": row['centroid_longitude'],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": [
                "temperature_2m", "relative_humidity_2m", "precipitation", "rain",
                "snowfall", "windspeed_10m", "winddirection_10m"
            ],
            "timezone": "auto"
        }

        try:
            responses = openmeteo.weather_api(url, params=params)
            # Process first location
            response = responses[0]

            # Use indices based on the API documentation
            hourly = response.Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
            hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
            hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
            hourly_rain = hourly.Variables(3).ValuesAsNumpy()
            hourly_snowfall = hourly.Variables(4).ValuesAsNumpy()
            hourly_windspeed_10m = hourly.Variables(5).ValuesAsNumpy()
            hourly_winddirection_10m = hourly.Variables(6).ValuesAsNumpy()

            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                ),
                "temperature_2m": hourly_temperature_2m,
                "relative_humidity_2m": hourly_relative_humidity_2m,
                "precipitation": hourly_precipitation,
                "rain": hourly_rain,
                "snowfall": hourly_snowfall,
                "windspeed_10m": hourly_windspeed_10m,
                "winddirection_10m": hourly_winddirection_10m
            }

            hourly_dataframe = pd.DataFrame(data=hourly_data)
            hourly_dataframe['ZIPCODE'] = row['ZIPCODE']  # Add ZIP code to the dataframe
            hourly_dataframe['Latitude'] = row['centroid_latitude']  # Add Latitude to the dataframe
            hourly_dataframe['Longitude'] = row['centroid_longitude']  # Add Longitude to the dataframe

            dfs.append(hourly_dataframe)

        except Exception as e:
            print("An error occurred:", e)
            print("Response content:", responses)

    # Concatenate all dataframes
    result = pd.concat(dfs, ignore_index=True)

    return result


def transform_weather_fact(result):
    """
    Generate a weather fact DataFrame for given ZIP codes within a specified date range.

    Args:
        result (pd.DataFrame): data extracted from open meteo api

    Returns:
        DataFrame: A DataFrame containing weather facts for the specified ZIP codes and date range.
    """
    # Generate unique keys
    result['LocationAreaKey'] = (
            result['Longitude'].astype(str).str.replace('.', '', regex=False).str.replace('-', '', regex=False).str[:8] +
            result['Latitude'].astype(str).str.replace('.', '', regex=False).str.replace('-', '', regex=False).str[:8]
    )
    result['LocationAreaKey'] = result['LocationAreaKey'].astype(np.int64)

    result['DateHourKey'] = result['date'].dt.strftime('%Y%m%d%H').astype(np.int64)

    result['WeatherKey'] = (
            result['DateHourKey'].astype(str)[2:] +
            result['Longitude'].astype(str).str.replace('.', '', regex=False).str.replace('-', '', regex=False).str[1:6] +
            result['Latitude'].astype(str).str.replace('.', '', regex=False).str.replace('-', '', regex=False).str[1:6]
    )
    result['WeatherKey'] = (result['LocationAreaKey'].astype(str) + '_' + result['date'].astype(str)).apply(hash).apply(abs)

    # Prepare the WeatherFact DataFrame
    WeatherFact = result.drop(columns=['date', 'Latitude', 'Longitude', 'ZIPCODE'])
    WeatherFact = WeatherFact.rename(columns={
        "temperature_2m": "Temperature",
        "relative_humidity_2m": "Humidity",
        "precipitation": "Precipitation",
        "rain": "Rain",
        "snowfall": "Snow",
        "windspeed_10m": "WindSpeed",
        "winddirection_10m": "WindDirection"
    })

    return WeatherFact
