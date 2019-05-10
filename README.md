# README

Author: Andrew Dieken
Date: 04/29/2019

### Running Main

The application can be found in `main.py`.
- `main.py` uses helper functions found in `database_helper.py`

To use your own schema & data, simply replace the contents of the files

-  `schema.csv`
-  `data.csv`

found in the `data_drop` folder with your own.


To run the application, run the following (ensure you are in the `data_dump` directory)
```shell
$ python main.py
```

#### Main flow

1) Connects to MySQL instance
2) Creates database if it does not exists OR uses database if it does
3) Creates table if it does not exists
4) Creates an array of all attributes
5) Populates the table

### Running Tests

The tests can be found in `mysql_unittests.py`.

To run the tests, run the following (ensure you are in the `data_dump` directory)
```shell
$ python mysql_unittests.py
```

### Database Type

MySQL

### Connection Mechanism

Python MySQL Connector

To use the Python MySQL Connector, run the following
```shell
$ pip install mysql-connector
```

### Connection Credentials

The MySQL connection credentials can be found and changed in the `config.py` file.

MySQL credentials that can be configured include
- host
- user
- password
- database
- table

### Error Handling

Using the `errorcode` module from Python MySQL Connector

### Invalid Entries

When parsing `data.csv` and encountered invalid entries such as

- Invalid type
- String too long

entries were written to `invalid.csv` for further review.
