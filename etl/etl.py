class ETL:

    def __init__(self):
        self.crash_data = None
        self.weather_data = None
        # ...

    def extract_data(self):
        """ load data from different sources """
        pass

    def transform_data(self):
        """ run transformations """
        pass

    def join_data(self):
        """ generate foreign keys """
        pass

    def load_data(self):
        """ load data to dwh"""
        pass