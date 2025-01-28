import oracledb
import MetroDeMadrid_db
import pandas as pd #pandas will help export the files to the oracle database

connection = MetroDeMadrid_db.get_connection

#string for the directory of all the gtfs files, for simplicity
metroPath = "/data/MetroDeMadrid/gtfs/"

#list of all the files inside of the gtfs data folder
gtfs_files = {
    f"{metroPath}agency.txt": "stops",
    f"{metroPath}calendar.txt": "calendar",
    f"{metroPath}calendar.txt": "calendar_dates",
    f"{metroPath}calendar.txt": "fare_attributes",
    f"{metroPath}calendar.txt": "fare_rules",
    f"{metroPath}calendar.txt": "feed_info",
    f"{metroPath}calendar.txt": "frequencies",
    f"{metroPath}routes.txt": "routes",
    f"{metroPath}routes.txt": "shapes",
    f"{metroPath}stops.txt": "stops",
    f"{metroPath}stop_times.txt": "stop_times",
    f"{metroPath}trips.txt": "trips"
}
    
connection.close()