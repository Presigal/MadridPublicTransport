import MetroDeMadrid_db #file that contains the DB connection
import json #importing JSON to read in MetroMadridTables.JSON
import oracledb
import sys

errorTables = []

#Opening a logging file for anything that comes out of this file
log_file = open("logs/createTablesMetro_.log", "w")
sys.stdout = log_file
print("Starting the program...")

connection = MetroDeMadrid_db.get_connection()  #connecting to the database. Imported the function for security
cursor = connection.cursor()    #initializing the cursor
print("connection established!\n")

#reading in the JSON
with open('data/MetroDeMadrid/gtfs/MetroMadridTables.json', 'r') as file:
    MetroTables = json.load(file)

print(f"The entirety of MetroTables:\n{MetroTables}")

#This for loop will iterate through the different table names
#When inside of a "table", it will take the dictionary items and create the tables accordingly.
for table_name, table_details in MetroTables.items():
    metroColumns = table_details["columns"]

    #printing to see if its seeing it correctly
    print(f"Creating table {table_name}\n")

    # These two statements will be used to write the string for the Oracle command
    columns_definitions = ", ".join([f"{col} {dtype}" for col, dtype in metroColumns.items()])
    print("*******columns_definitions**********")
    print(f"{columns_definitions}\n")
    create_table_sql = f"CREATE TABLE {table_name} ({columns_definitions})"

    print("**********create_table_sql*************")
    print(f"{create_table_sql}\n")

    try:
        #this is the command itself. We are going to pass the string as an Oracle Command
        cursor.execute(create_table_sql)
        print(f"Table {table_name} created successfully.\n")
        print("__________________________________________________________________")

    #error handling in case it doesnt work
    except oracledb.DatabaseError as e:
        print(f"Error creating table {table_name}: {e}\n")
        errorTables.append(table_name)
        print("__________________________________________________________________")

cursor.close()
connection.close()

print(f"Tables with creation issues: {', '.join(errorTables)}")

print("Connection closed successfully!")