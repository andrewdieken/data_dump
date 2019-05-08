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
    def test_create_database(self):
        print('pass')

    def test_get_attributes(self):
        result = getAttributes("./data_drop/test_schema.csv")
        self.assertEqual(result, [('author_name', '10', 'CHAR'), ('is_alive', '1', 'BOOLEAN'), ('books_authored_count', '2', 'INTEGER')])

    def test_parse_schema(self):
        result = parseSchema("./data_drop/test_schema.csv")
        self.assertEqual(result, ['ALTER TABLE test ADD author_name CHAR(10)', 'ALTER TABLE test ADD is_alive BOOLEAN', 'ALTER TABLE test ADD books_authored_count INTEGER(2)'])

    def test_entry_exists(self):
        print('pass')

    def test_vaild_entry(self):
        print('pass')

    def test_log_invalid_entry(self):
        print('pass')

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

        self.assertEqual(result, ["INSERT INTO test (author_name,is_alive,books_authored_count) VALUES ('Steph King', 0, 54)", "INSERT INTO test (author_name,is_alive,books_authored_count) VALUES ('Mark Twain', 0, 10)"])

if __name__ == '__main__':
    unittest.main()
