# Configure SQL Server connection

#importing module Like Namespace in .Net   
import pypyodbc	
import json
from datetime import datetime

#creating connection Object which will contain SQL Server Connection	
connection_string = ''   


# Function to save Temperature to DB Table
def tempDataHandler(jsonData, connection_string):

	#Parse JSON data from sensor
	jsonLoad = json.loads(jsonData)
	
	# Extract data from the imported JSON
	sensorID = jsonLoad['sensorID']
	#readingTimestamp = json_Dict['readingTimestamp']
	data = jsonLoad['data']
	measurementUnits = jsonLoad["measurementUnits"]
	readingTimestamp = datetime.now()

	# Open database connection
	conn = pypyodbc.connect(connection_string)
	print("SQL Database Connected.") 
	
	# Insert data into SQL Server
	cursor = conn.cursor()
	# Prepare SQL insert statement
	#sqlCommand = ("INSERT INTO temperature(sensorID, readingTimestamp, sensorValue, measurementUnits) VALUES (?,?,?,?)")
	sqlCommand = ("EXEC insertTemperature @sensorID = ?, @readingTimestamp = ?, @sensorValue = ?, @measurementUnits = ?")
	# Bind values from the MQTT message
	values = [sensorID, readingTimestamp, data, measurementUnits]

	# Execute the query
	cursor.execute(sqlCommand,values)
	print("Commit transaction")
	# Commiting any pending transactions to the database.
	conn.commit()
	# Close the database connection
	conn.close()
	del conn

# Function to save Humidity to DB Table
def lightDataHandler(jsonData, connection_string):
	
	#Parse JSON data from sensor
	jsonLoad = json.loads(jsonData)
	
	# Extract data from the imported JSON
	sensorID = jsonLoad['sensorID']
	#readingTimestamp = json_Dict['readingTimestamp']
	data = jsonLoad['data']
	measurementUnits = jsonLoad["measurementUnits"]
	readingTimestamp = datetime.now()

	# Open database connection
	conn = pypyodbc.connect(connection_string)
	print("SQL Database Connected.")
	# Insert data into SQL Server
	cursor = conn.cursor()

	# Prepare SQL insert statement
	#sqlCommand = ("INSERT INTO light(sensorID, readingTimestamp, sensorValue, measurementUnits) VALUES (?,?,?,?)")
	sqlCommand = ("EXEC insertLight @sensorID = ?, @readingTimestamp = ?, @sensorValue = ?, @measurementUnits = ?")
	# Bind values from the MQTT message
	values = [sensorID, readingTimestamp, data, measurementUnits]

	# Execute the query
	cursor.execute(sqlCommand,values)
	print("Commit transaction")
	# Commiting any pending transactions to the database.
	conn.commit()
	# Close the database connection
	conn.close()
	del conn

#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensorDataHandler (topic, jsonData):
	if topic == "iot/data/temperature":
		tempDataHandler(jsonData, connection_string)
	elif topic == "iot/data/light":
		lightDataHandler(jsonData, connection_string)

#===============================================================
