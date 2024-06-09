import os

from dotenv import load_dotenv

load_dotenv()


class Config:

    DWH_INITIALIZATION = True
    """ if this is the first time laoding data into data warehouse """

    FROM_FILES = False
    """ if pipeline is run from local files instead api """

    DEBUG = True

    SOTA_TOKEN = os.getenv("SOTA_TOKEN")
    """ Montgomery data portal API token """

    SOTA_USER = os.getenv("SOTA_USER")
    """ Montgomery data portal user login """

    SOTA_PWD = os.getenv("SOTA_PWD")
    """ Montgomery data portal user password """

    DRIVER_NAME = os.getenv("DWH_DRIVER_NAME", default='SQL SERVER')
    """ SQL connection driver name """

    SERVER_NAME = os.getenv("DWH_SERVER_NAME")
    """ SQL Server server name """

    DATABASE_NAME = os.getenv("DWH_DATABASE_NAME", default='vehicle_crashes_dwh')
    """ database name """

    DWH_USER = os.getenv("DWH_USER")
    """ SQL Server user login """

    DWH_PASSWORD = os.getenv("DWH_PASSWORD")
    """ SQL Server user password """
