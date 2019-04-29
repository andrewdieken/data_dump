# README

Author: Andrew Dieken
Date: 04/29/2019

### Database Type

MySQL

### Connection Mechanism

Python MySQL Connector

```shell
pip install mysql-connector
```

### Connection Credentials

The MySQL connection credentials can be found and changed in the `config.py` file.

MySQL credentials that can be configured include
- host
- user
- password
- database

### Error Handling

Using the `errorcode` module from Python MySQL Connector

### Invalid Entries

When parsing `data.csv` and encountered invalid entries such as

- Invalid type
- Field width out of range

entries were written to `invalid.csv` with an error message as to why the entry was invalid.
