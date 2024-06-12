# Montgomery County Vehicle Crashes Data Warehouse

Full ETL Python pipeline configured with SQL Server database.

## Data sources:

### Montgomery County Open Data
[Incidents Data](https://data.montgomerycountymd.gov/Public-Safety/Crash-Reporting-Incidents-Data/bhju-22kf/about_data)<br>
[Drivers Data](https://data.montgomerycountymd.gov/Public-Safety/Crash-Reporting-Drivers-Data/mmzv-x632/about_data) <br>
[Non-motorists Data](https://data.montgomerycountymd.gov/Public-Safety/Crash-Reporting-Non-Motorists-Data/n7fk-dce5) <br>
warning: deprecated API, ETL works only on 'emergency' branch with data being loaded from flat files

### fueleconomy.gov
[Vehicles](https://www.fueleconomy.gov/feg/ws/index.shtml)

### Open Meteo
[API](https://open-meteo.com/en/docs/historical-weather-api)

### Geographical data
[Zipcodes](https://catalog.data.gov/dataset/zipcodes)
