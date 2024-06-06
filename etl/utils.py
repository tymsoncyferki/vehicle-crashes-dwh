import os

from dotenv import load_dotenv
from sodapy import Socrata
import pandas as pd


load_dotenv('../.env')


def soda_montgomery_request(dataset, start_date, end_date):
    """
    Fetch data from montgomery county data portal for given dataset and date interval

    Args:
        dataset (str): type of dataset to pull, available options: 'incidents', 'drivers', 'non-motorists'
        start_date (str): The start date for data retrieval in "YYYY-MM-DD HH:MM:SS" format.
        end_date (str): The end date for the data retrieval in "YYYY-MM-DD HH:MM:SS" format.

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

    where_clause = f"crash_date_time >= '{start_date}' AND crash_date_time <= '{end_date}'"

    results = client.get(data_key, where=where_clause, limit=1000000)
    results_df = pd.DataFrame.from_records(results)

    return results_df
