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
# Ex) [('author_name', '10', 'CHAR'), ('is_alive', '1', 'BOOLEAN'), ('books_authored_count', '2', 'INTEGER')]
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
    # Check if entry has all attributes
    if len(entry) != len(attributes):
        return False
    else:
        for i in range(len(entry)):
            # if char
            if attributes[i][2] == "CHAR":
                return len(entry[i]) <= int(attributes[i][1])
            # if int
            if attributes[i][2] == "INTEGER":
                return len(str(entry[i])) <= len(str(attributes[i][1]))
            # boolean
            if attributes[i][2] == "BOOLEAN":
                return entry[i] <=  1

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
    # Assemble Insert String
    attributeSting = ""
    for i in range(len(attributes)):
        if i == (len(attributes) - 1):
            attributeSting += attributes[i][0]
        else:
            attributeSting += attributes[i][0] + ","

    insertString = "INSERT INTO {} (".format(credentials.table) + attributeSting + ") VALUES ({})"

    try:
        with open(fileName, 'r', encoding="utf8") as csvfile:
            csvreader = csv.reader(csvfile)
            for entry in csvreader:
                if entryExists(cursor, entry):
                    if validEntry(attributes, entry):
                        # Build insertString for entry
                        valueString = ""
                        for i in range(len(entry)):
                            # Check if value type is CHAR
                            if isChar(attributes[i][2]):
                                if i == (len(entry) - 1):
                                    valueString += "\'" + entry[i] + "\'"
                                else:
                                    valueString += "\'" + entry[i] + "\'" + ","
                            else:
                                if i == (len(entry) - 1):
                                    valueString += entry[i]
                                else:
                                    valueString += entry[i] + ","
                        returnArray.append(insertString.format(valueString))
                    else:
                        print("Logging entry to 'invalid.csv' for review.\n")
                        logInvalidEntry(entry)
                else:
                    print("ERROR: entry already exists.\n")

        csvfile.close()
        return returnArray

    except csv.Error as error:
        print("FILE ERROR: {}".format(error))

#===============================================================================
# Checks if a given attribute type is CHAR
# -> True if type is CHAR
# -> False if not
#===============================================================================
def isChar(attribute):
    if attribute == "CHAR":
        return True
    else:
        return False
