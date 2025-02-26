import MetroDeMadrid_db
import pandas as pd #pandas will help export the files to the oracle database
import sys
import datetime

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
            df = renameDate(df) #function to rename date to "date" to avoid Oracle keyword date

        if  not df.empty:
            if 'start_time' in df.columns or 'end_time'  or 'arrival_time' or 'departure_time' in df.columns:
                startEndTime(df)
                
            else:
                # Prepare the SQL insert statement
                columns = ', '.join(df.columns)
                placeholders = ', '.join([f':{i+1}' for i in range(len(df.columns))])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                print(f"This is the following SQL Statement:\n{sql}")
                
                # Insert data row by row
                with connection.cursor() as cursor:
                    rows = [tuple(row) for row in df.itertuples(index=False)]
                    cursor.executemany(sql, rows)
                    connection.commit()

            successfulTables.append(table_name) #adding tables that completed without errors to this array
            print(f"Data from {file_path} imported successfully into {table_name}.\n")

        else:
            print(f"Skipping {table_name}....\n")
            skippedTables.append(table_name)

    except Exception as e:
        print(f"Error importing {file_path}: {e}\n")
        failedTables.append(table_name)

def renameDate(df_):
    print("Replacing column data with 'date'...\n")
    df_ = df_.rename(columns={'date': '"date"'})
    return df_
            
#This function is to correctly import the DATE values into SQL. Used for the "Frequencies" table
def startEndTime(df__):
    if 'start_time' in df__.columns or 'end_time' in df__.columns:

        df__["start_time"] = df__["start_time"].apply(convert_to_timestamp)
        df__["end_time"] = df__["end_time"].apply(convert_to_timestamp)

        # Use TO_TIMESTAMP to convert the string into a proper TIMESTAMP
        query = """
        INSERT INTO frequencies (trip_id, start_time, end_time, headway_secs, exact_times) 
        VALUES (:1, TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(:3, 'YYYY-MM-DD HH24:MI:SS'), :4, :5)
        """

        data_tuples = [tuple(row) for row in df__.itertuples(index=False, name=None)]
        
        with connection.cursor() as cursor_:
                for row in data_tuples:
                    cursor_.execute(query, row)

        connection.commit()

def convert_to_timestamp(time_str):
    if not pd.isna(time_str):
        #the map function allows us to seperate the three seperate values of the timestamp. Will be useful for checking anything over 23 hrs.
        hours, minutes, seconds = map(int, time_str.split(":"))

        #using the split values, we can now measure hours independently from the rest of the timestamp. Oracle doesnt like any timestamp above 24 hours, so we will roll over those values to the next day. 
        if hours >= 24:
            hours_ = hours - 24  # Subtract 24 to make it valid
            return f"1970-01-02 {hours_:02}:{minutes:02}:{seconds:02}"  # Move to next day
        
        return f"1970-01-01 {hours:02}:{minutes:02}:{seconds:02}"  # Normal case    
    
    else:
        return None


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