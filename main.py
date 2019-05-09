import config as credentials
import database_helper
import mysql.connector
from mysql.connector import errorcode


if __name__ == '__main__':

    schemaFilePath = "./data_drop/schema.csv"
    dataFilePath = "./data_drop/data.csv"
    databaseName = credentials.database
    attributes = []

    #===============================================================================
    # Connect to MySQL
    #===============================================================================
    try:
        mySQL = mysql.connector.connect(
            host=credentials.host,
            user=credentials.user,
            password=credentials.password
        )
        mySQLCursor = mySQL.cursor()

    except mysql.connector.Error as error:
        print("CONNECTION ERROR: ", end='')
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Invalid login credentials")
        else:
            print(error)
        exit(1)

    #===============================================================================
    # Create Database
    #===============================================================================
    try:
        mySQLCursor.execute("USE {}".format(databaseName))
        print("Database '{}' already exists.".format(databaseName))
        print("Successfully using database: {}\n".format(databaseName))

    except mysql.connector.Error as error:
        print("ERROR: Database '{}' does not exists.".format(databaseName))

        if (error.errno == errorcode.ER_BAD_DB_ERROR):
            database_helper.createDatabase(mySQLCursor, databaseName)
            print("Database '{}' created successfully.\n".format(databaseName))
            mySQL.database = databaseName

        else:
            print("ERROR: {}\n".format(error))
            exit(1)

    #===============================================================================
    # Create Table
    #===============================================================================
    try:
        print("Creating table {}: ".format(credentials.table), end='')
        schemaString = ("CREATE TABLE {}(id INT AUTO_INCREMENT PRIMARY KEY)".format(credentials.table))
        mySQLCursor.execute(schemaString)

        alterStatements = database_helper.parseSchema(schemaFilePath)
        for statement in alterStatements:
            mySQLCursor.execute(statement)
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.\n")
        else:
            print(error.msg)
    else:
        print("OK\n")

    #===============================================================================
    # Get Attributes
    #===============================================================================
    attributes = database_helper.getAttributes(schemaFilePath)

    #===============================================================================
    # Populate Table
    #===============================================================================
    try:
        print("Populating table '{}':\n".format(credentials.table), end='')

        insertStatements = database_helper.parseData(mySQLCursor, attributes, dataFilePath)
        for statement in insertStatements:
            mySQLCursor.execute(statement)

            # Make changes to database
            mySQL.commit()
    except mysql.connector.Error as error:
        print(error.msg)
    else:
        print("OK")
