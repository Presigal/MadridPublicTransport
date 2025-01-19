import MetroDeMadrid_db #file that contains the DB connection
import json #importing JSON to read in MetroMadridTables.JSON

#reading in the JSON
with open('tables.json', 'r') as file:
    table_definitions = json.load(file)

try:
    #connecting to the database. Imported the function for security
    connection = MetroDeMadrid_db.get_connection()
    #initializing the cursor
    cursor = connection.cursor()

    #The below query is to fetch all the information from the table #stops
    query = "" #command to create all the tables

    # Execute the query
    cursor.execute(query)
    
    rows = cursor.fetchall() # Fetch all rows
    columns = [desc[0] for desc in cursor.description]  #extract the column names, as the above query doesnt return column names

    cursor.close()
    connection.close()
    print("Connection closed successfully!")
    
except Exception as e:
    print(f"Error connecting to the database: {e}")