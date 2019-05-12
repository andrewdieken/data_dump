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
# Parses `schema.csv` file and returns the attributes as an array of tuples && and array of the indexes of attributes of type CHAR
# Return => [('author_name', '10', 'CHAR'), ('is_alive', '1', 'BOOLEAN'), ('books_authored_count', '2', 'INTEGER')]
#        => [0]
#===============================================================================
def getAttributes(fileName):
    attributes = []
    charIndexes = []
    try:
        with open(fileName, 'r', encoding='utf8') as csvFile:
            csvReader = csv.reader(csvFile)
            next(csvReader)
            index = 0
            for attribute in csvReader:
                attributes.append((attribute[0],attribute[1], attribute[2]))
                if "CHAR" in attribute:
                    charIndexes.append(index)
                index += 1

        csvFile.close()

        return attributes, charIndexes
    except csv.Error as error:
        print("FILE ERROR: {}".format(error))

#===============================================================================
# Parses `schema.csv` file and returns array of ALTER statement strings to be
# executed in `main.py`
# Return => ['ALTER TABLE test ADD author_name CHAR(10)', 'ALTER TABLE test ADD is_alive BOOLEAN', 'ALTER TABLE test ADD books_authored_count INTEGER(2)']
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
#
#===============================================================================
def validEntry(attributes, entry):
    # Check if entry has all attributes
    if len(entry) != len(attributes):
        return False
    else:
        for i in range(len(entry)):
            # if char
            if attributes[i][2] == "CHAR":
                if len(entry[i]) > int(attributes[i][1]):
                    return False, "CHAR length too large"
            # if int
            if attributes[i][2] == "INTEGER":
                if len(str(entry[i])) > int(attributes[i][1]):
                    return False, "INTEGER too large"
            # boolean
            if attributes[i][2] == "BOOLEAN":
                if int(entry[i]) >  1:
                    return False, "BOOLEAN value invalid. Not 1 or 0"

        return True, ""

#===============================================================================
# Writes entry to `invalid.csv` for review
# -> True, "" if no errors
# -> False, "CHAR length too large" if entry attribute is type CHAR && length is larger than schema
# -> False, "INTEGER too large" if entry attribute is type INTEGER && size if larger than schema
# -> False, "BOOLEAN value invalid. Not 1 or 0" if entry attribute is type BOOLEAN && value is not 1 or 0
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
# Ex) Entry => ['Steph King', '0', '54']
# Return => ["INSERT INTO test (author_name,is_alive,books_authored_count) VALUES ('Steph King',0,54)"]
#===============================================================================
def parseData(cursor, attributes, charIndexes, fileName):
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
                    valid, error = validEntry(attributes, entry)
                    if valid:
                        for index in charIndexes:
                            entry[index] = "\'" + entry[index] + "\'"

                        returnArray.append(insertString.format(",".join(entry)))
                    else:
                        print("Logging entry to 'invalid.csv' for review.\n")
                        # Add error to entry
                        entry.append(error)
                        logInvalidEntry(entry)
                else:
                    print("ERROR: entry already exists.\n")

        csvfile.close()
        return returnArray

    except csv.Error as error:
        print("FILE ERROR: {}".format(error))
