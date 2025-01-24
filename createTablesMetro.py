import MetroDeMadrid_db #file that contains the DB connection
import json #importing JSON to read in MetroMadridTables.JSON
import time

#reading in the JSON

with open('data/MetroDeMadrid/gtfs/MetroMadridTables.json', 'r') as file:
    MetroTables = json.load(file)

#This for loop creates a Dictionary, for the columns inside the tables have associated data types
#for table_name, table_details in MetroTables.items():
#    metroDataTypes = table_details["columns"]

#print(metroDataTypes)

try:
    connection = MetroDeMadrid_db.get_connection()  #connecting to the database. Imported the function for security
    cursor = connection.cursor()    #initializing the cursor
    print("connection established!")

    # Construct the CREATE TABLE SQL statement
    columns_definitions = ", ".join([f"{col} {dtype}" for col, dtype in MetroTables.items()])
    create_table_sql = f"CREATE TABLE {table_name} ({columns_definitions})"


    cursor.close()
    connection.close()
    print("Connection closed successfully!")
    
except Exception as e:
    print(f"Error connecting to the database: {e}")