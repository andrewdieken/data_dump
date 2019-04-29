import config as credentials
import csv
import mysql.connector
from mysql.connector import errorcode

#===============================================================================
# Creates database with database name from `config.py`
#===============================================================================
def createDatabase(cursor, databaseName):
    try:
        cursor.execute("CREATE DATABASE {}".format(databaseName))
    except mysql.connector.Error as error:
        print("Failed creating database: {}".format(error))
        exit(1)

#===============================================================================
# Parses `schema.csv` file and returns the attributes as an array of tuples
#===============================================================================
def getAttributes(fileName):
    attributes = []
    try:
        with open(fileName, 'r', encoding='utf8') as csvFile:
            csvReader = csv.reader(csvFile)
            next(csvReader)
            for attribute in csvReader:
                attributes.append((attribute[0],attribute[1], attribute[2]))

        csvFile.close()

        return attributes
    except csv.Error as error:
        print("FILE ERROR: {}".format(error))

#===============================================================================
# Parses `schema.csv` file and returns array of ALTER statement strings to be
# executed in `main.py`
#===============================================================================
def parseSchema(fileName):
    returnArray = []
    alterString = "ALTER TABLE {} ADD {} {}"
    table = credentials.table

    try:
        with open(fileName, "r", encoding="utf8") as csvFile:
            csvReader = csv.reader(csvFile)
            # Skip header
            next(csvReader)
            for attribute in csvReader:
                attributeName = attribute[0]
                attributeType = attribute[2]

                if attributeType != "BOOLEAN":
                    attributeType = attributeType + "({})".format(attribute[1])

                returnArray.append(alterString.format(table, attributeName, attributeType))

        csvFile.close()
        return returnArray

    except csv.Error as error:
        print("FILE ERROR: {}".format(error))

#===============================================================================
# Checks table to ensure entry for duplicates
# Returns:
# -> True if there is no entry in table matching entry[0]
# -> False if none
#===============================================================================
def entryExists(cursor, entry):
    query = "SELECT EXISTS(SELECT * FROM {} WHERE author_name='{}')".format(credentials.table, entry[0])
    cursor.execute(query)
    result = cursor.fetchone()
    if result[0] > 0:
        return False
    else:
        return True

#===============================================================================
# Checks each entry in `data.csv`
#===============================================================================
def validEntry(attributes, entry):
    # Validate length of the name
    if len(str(entry[0])) > int(attributes[0][1]):
        print("ERROR: name too long.")
        return False
    elif int(entry[1]) < 0 or int(entry[1]) > 1:
        print("ERROR: not vaild BOOLEAN input.")
        return False
    elif int(entry[2]) > 99:
        print("ERROR: number too long.")
        return False
    else:
        return True

#===============================================================================
# Writes entry to `invalid.csv` for review
#===============================================================================
def logInvalidEntry(entry):
    try:
        with open("invaild.csv", "a", encoding="utf8") as csvfile:
            csvWriter = csv.writer(csvfile)
            csvWriter.writerow(entry)

    except csv.Error as error:
        print("FILE ERROR: {}.".format(error))

#===============================================================================
# Parses `data.csv` and returns array of INSERT statement strings to be executed
# in `main.py`
#===============================================================================
def parseData(cursor, attributes, fileName):
    returnArray = []
    insertString = "INSERT INTO {} (author_name, is_alive, books_authored_count) VALUES ('{}', {}, {})"
    table = credentials.table
    try:
        with open(fileName, 'r', encoding="utf8") as csvfile:
            csvreader = csv.reader(csvfile)
            for entry in csvreader:
                if entryExists(cursor, entry):
                    if validEntry(attributes, entry):
                        attribute1 = entry[0]
                        attribute2 = entry[1]
                        attribute3 = entry[2]
                        returnArray.append(insertString.format(table, attribute1, attribute2, attribute3))
                    else:
                        print("Logging entry to 'invalid.csv' for review.\n")
                        logInvalidEntry(entry)
                else:
                    print("ERROR: entry already exists.\n")

        csvfile.close()
        return returnArray

    except csv.Error as error:
        print("FILE ERROR: {}".format(error))
