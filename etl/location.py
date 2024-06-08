import pandas as pd
import numpy as np
from shapely.wkt import loads as load_wkt
import geopandas as gpd


def generate_location_area_dim(zipcodes):
    """
    Generate a location area dimension DataFrame from the given zipcodes DataFrame.

    Args:
        zipcodes (DataFrame): A DataFrame containing ZIP codes and their geometries.

    Returns:
        DataFrame: A DataFrame containing the location area dimension data.
    """
    # Convert 'the_geom' to geometry and calculate centroids
    zipcodes['geometry'] = zipcodes['the_geom'].apply(load_wkt)
    gdf = gpd.GeoDataFrame(zipcodes, geometry='geometry')
    gdf['centroid'] = gdf['geometry'].centroid
    gdf['centroid_latitude'] = gdf['centroid'].y
    gdf['centroid_longitude'] = gdf['centroid'].x

    # Generate LocationAreaKey by concatenating latitude and longitude without special characters
    gdf['LocationAreaKey'] = (
        gdf['centroid_longitude'].astype(str).str.replace('.', '', regex=False).str.replace('-', '', regex=False).str[:8] +
        gdf['centroid_latitude'].astype(str).str.replace('.', '', regex=False).str.replace('-', '', regex=False).str[:8]
    )

    # Prepare the location area dimension DataFrame
    location_area_dim = pd.DataFrame({
        'LocationAreaKey': gdf['LocationAreaKey'].astype(np.int64),
        'Zipcode': gdf['ZIPCODE'],
        'MailCity': gdf['MAIL_CITY'],
        'ShapeLength': gdf['Shape_Leng'],
        'ShapeArea': gdf['Shape_Area'],
        'CentroidLatitude': gdf['centroid_latitude'],
        'CentroidLongitude': gdf['centroid_longitude']
    })

    unknown_row = pd.DataFrame({
        'LocationAreaKey': 0,
        'Zipcode': 0,
        'MailCity': 'Unknown',
        'ShapeLength': 0,
        'ShapeArea': 0,
        'CentroidLatitude': 0,
        'CentroidLongitude': 0
    }, index=[0])

    location_area_dim = pd.concat([location_area_dim, unknown_row], ignore_index=True)

    return location_area_dim
