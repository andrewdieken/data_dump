import unittest
import mysql.connector
from database_helper import createDatabase
from database_helper import getAttributes
from database_helper import parseSchema
from database_helper import entryExists
from database_helper import validEntry
from database_helper import logInvalidEntry
from database_helper import parseData

class TestMysql(unittest.TestCase):
    #===============================================================================
    # Test for `createDatabase()` function
    #===============================================================================
    def test_create_database(self):
        print('pass')

    #===============================================================================
    # Test for `getAttributes()` function
    #===============================================================================
    def test_get_attributes(self):
        result = getAttributes("./data_drop/test_schema.csv")
        self.assertEqual(result, [('author_name', '10', 'CHAR'), ('is_alive', '1', 'BOOLEAN'), ('books_authored_count', '2', 'INTEGER')])

    #===============================================================================
    # Test for `parseSchema()` function
    #===============================================================================
    def test_parse_schema(self):
        result = parseSchema("./data_drop/test_schema.csv")
        self.assertEqual(result, ['ALTER TABLE test ADD author_name CHAR(10)', 'ALTER TABLE test ADD is_alive BOOLEAN', 'ALTER TABLE test ADD books_authored_count INTEGER(2)'])

    #===============================================================================
    # Test for `entryExists()` function
    #===============================================================================
    def test_entry_exists(self):
        mySQL = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="DataDump"
        )
        cursor = mySQL.cursor()
        entry = ['Mark Twain', '0', '10']
        result = entryExists(cursor, entry)

        self.assertEqual(result, False)

    #===============================================================================
    # Test for `validEntry()` function
    #===============================================================================
    def test_vaild_entry(self):
        attributes = getAttributes("./data_drop/test_schema.csv")
        entry = ['Michael Cricton', '1', '28']
        result = validEntry(attributes, entry)

        self.assertEqual(result, False)

    #===============================================================================
    # Test for `parseData()` function
    #===============================================================================
    def test_parse_data(self):
        mySQL = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="DataDump"
        )
        cursor = mySQL.cursor()
        attributes = getAttributes("./data_drop/test_schema.csv")
        result = parseData(cursor, attributes, "./data_drop/test_data.csv")

        self.assertEqual(result, ["INSERT INTO test (author_name,is_alive,books_authored_count) VALUES ('Steph King',0,54)"])

if __name__ == '__main__':
    unittest.main()
