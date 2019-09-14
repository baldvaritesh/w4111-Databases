# I write and test methods one at a time.
# This file contains unit tests of individual methods.

import logging
import os


def logger_load():
    # The logging level to use should be an environment variable, not hard coded.
    logging.basicConfig(level=logging.DEBUG)

    # Also, the 'name' of the logger to use should be an environment variable.
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    return logger


def data_dir(path):
    # This should also be an environment variable.
    # Also not the using '/' is OS dependent, and windows might need `\\`
    return os.path.abspath(path)


def t_load(class_name, file_name="People.csv", table_name="people"):
    connect_info = {
        "directory": data_dir("../Data/Baseball"),
        "file_name": file_name,
    }

    csv_tbl = class_name(table_name, connect_info, ["playerID"])

    print("Created table = " + str(csv_tbl))
    return csv_tbl


# Returns the connection
def sql_load(class_name, database_name="baseball", table_name="people"):
    connect_info = {
        "host": "localhost",
        "user": "dbuser",
        "password": "databases4111",
        "db": "baseball"
    }

    rdb_table = class_name(table_name, connect_info, ["playerID"])

    print("Created instance of RDBTable and connected to Database")
    return rdb_table


# Shouldn't need this now.
# t_load()
