import oracledb
import MetroDeMadrid_db
import pandas as pd #pandas will help export the files to the oracle database
import sys

#This part is dedicated to logging any messages that come out of this program
log_file = open("logs/DataLoader_.log", "w")
sys.stdout = log_file
print("Starting the program...")

#opening the connection to the database
connection = MetroDeMadrid_db.get_connection()
print("connection established!\n")

successfulTables = []
failedTables = []
skippedTables = []

#This function will be used to load up all the contents of each GTFS file into each respective table
def load_gtfs_files(file_path, table_name):
    try:
        # Load the GTFS file into a pandas DataFrame
        df = pd.read_csv(file_path)
        df = df.fillna("") #fill the NULLs with empty spaces

        if 'date' in df.columns: #"date" is a reserved word in Oracle. This is to add quotes around date to be able to pass the data into the Oracle database.
            print("Replacing column data with 'date'...")
            df = df.rename(columns={'date': '"date"'})

        if  not df.empty:
            # Prepare the SQL insert statement
            columns = ', '.join(df.columns)
            placeholders = ', '.join([f':{i+1}' for i in range(len(df.columns))])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Insert data row by row
            with connection.cursor() as cursor:
                rows = [tuple(row) for row in df.itertuples(index=False)]
                cursor.executemany(sql, rows)
                connection.commit()

                successfulTables.append(table_name) #adding tables that completed without errors to this array
            
                print(f"Data from {file_path} imported successfully into {table_name}.\n")

        else:
            print(f"Skipping {table_name}....")
            skippedTables.append(table_name)

    except Exception as e:
        print(f"Error importing {file_path}: {e}\n")
        failedTables.append(table_name)

#string for the directory of all the gtfs files, for simplicity
metroPath = "data/MetroDeMadrid/gtfs/"

#list of all the files inside of the gtfs data folder
gtfs_files = {
    f"{metroPath}agency.txt": "agency",
    f"{metroPath}calendar.txt": "calendar",
    f"{metroPath}calendar_dates.txt": "calendar_dates",
    f"{metroPath}fare_attributes.txt": "fare_attributes",
    f"{metroPath}fare_rules.txt": "fare_rules",
    f"{metroPath}feed_info.txt": "feed_info",
    f"{metroPath}frequencies.txt": "frequencies",
    f"{metroPath}routes.txt": "routes",
    f"{metroPath}shapes.txt": "shapes",
    f"{metroPath}stops.txt": "stops",
    f"{metroPath}stop_times.txt": "stop_times",
    f"{metroPath}trips.txt": "trips"
}

for file_path, table_name in gtfs_files.items():
    load_gtfs_files(file_path, table_name)

print(f"Tables successfully copied data to: {successfulTables}\n")
print(f"Tables failed to copy to: {failedTables}\n")
print(f"Tables skipped: {skippedTables}\n")

connection.close()
print("connection successfully closed!")