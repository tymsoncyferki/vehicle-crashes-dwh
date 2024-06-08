import pandas as pd
import numpy as np
from datetime import date
import holidays


def generate_date_hour_dim(start_date="2023-12-01 00:00:00", end_date="2024-12-31 23:00:00"):
    """
    Generate a date-hour dimension DataFrame for a specified date range.

    Args:
        start_date (str): The start date for the date range in "YYYY-MM-DD HH:MM:SS" format.
        end_date (str): The end date for the date range in "YYYY-MM-DD HH:MM:SS" format.

    Returns:
        DataFrame: A DataFrame containing the date-hour dimension data.
    """

    # Generate a date range with hourly frequency
    date_range = pd.date_range(start=start_date, end=end_date, freq='h')

    # Create the DateHourDim DataFrame
    date_hour_dim = pd.DataFrame(date_range, columns=['Datetime'])

    # Populate the columns
    date_hour_dim['DateHourKey'] = date_hour_dim['Datetime'].dt.strftime('%Y%m%d%H').astype(int)
    date_hour_dim['Hour'] = date_hour_dim['Datetime'].dt.hour
    date_hour_dim['TimeOfDay'] = np.where(date_hour_dim['Hour'] < 12, 'AM', 'PM')
    date_hour_dim['DayNumber'] = date_hour_dim['Datetime'].dt.dayofyear
    date_hour_dim['WeekDayNumber'] = date_hour_dim['Datetime'].dt.weekday
    date_hour_dim['WeekDayName'] = date_hour_dim['Datetime'].dt.strftime('%A')
    date_hour_dim['WeekendFlag'] = np.where(date_hour_dim['WeekDayNumber'] >= 5, 1, 0)
    date_hour_dim['MonthNumber'] = date_hour_dim['Datetime'].dt.month
    date_hour_dim['MonthName'] = date_hour_dim['Datetime'].dt.strftime('%B')
    date_hour_dim['Year'] = date_hour_dim['Datetime'].dt.year

    # Get US holidays
    us_holidays = holidays.US()

    # Function to determine if a date is a holiday
    def is_holiday(date):
        date_only = date.date()
        if date_only in us_holidays:
            return 1, us_holidays.get(date_only)
        return 0, 'None'  # Using 'None' as a placeholder for no holiday

    # Apply the holiday function to determine holiday flags and names
    date_hour_dim['HolidayFlag'], date_hour_dim['HolidayName'] = zip(*date_hour_dim['Datetime'].apply(is_holiday))

    # Ensure the WeekendFlag and HolidayFlag are binary (0 or 1)
    date_hour_dim['WeekendFlag'] = date_hour_dim['WeekendFlag'].astype(int)
    date_hour_dim['HolidayFlag'] = date_hour_dim['HolidayFlag'].astype(int)

    # Drop duplicates to ensure unique DateHourKey entries
    date_hour_dim = date_hour_dim.drop_duplicates(subset=['DateHourKey'])

    # Display the DateHourDim DataFrame (for debugging purposes)
    # print(date_hour_dim)

    # Return the DateHourDim DataFrame
    return date_hour_dim
