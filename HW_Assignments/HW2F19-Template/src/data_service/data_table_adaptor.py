import pymysql
import src.data_service.dbutils as dbutils
import src.data_service.RDBDataTable as RDBDataTable

# The REST application server app.py will be handling multiple requests over a long period of time.
# It is inefficient to create an instance of RDBDataTable for each request.  This is a cache of created
# instances.
_db_tables = {}

# Default connection info
_default_connect_info = {
    'host': 'localhost',
    'user': 'root',
    'password': 'databases4111',
    'port': 3306
}


def get_rdb_table(table_name, db_name, key_columns=None, connect_info=None):
    """

    :param table_name: Name of the database table.
    :param db_name: Schema/database name.
    :param key_columns: This is a trap. Just use None.
    :param connect_info: You can specify if you have some special connection, but it is
        OK to just use the default connection.
    :return:
    """
    global _db_tables

    # We use the fully qualified table name as the key into the cache, e.g. lahman2019clean.people.
    key = db_name + "." + table_name

    # Have we already created and cache the data table?
    result = _db_tables.get(key, None)

    # We have not yet accessed this table.
    if result is None:

        # Make an RDBDataTable for this database table.
        result = RDBDataTable.RDBDataTable(table_name, db_name, key_columns, connect_info)

        # Add to the cache.
        _db_tables[key] = result

    return result


#########################################
#
#
# YOU HAVE TO IMPLEMENT THE FUNCTIONS BELOW.
#
#
# -- TO IMPLEMENT --
#########################################

def get_databases():
    """

    :return: A list of databases/schema at this endpoint.
    """
    global _default_connect_info
    cnx = dbutils.get_connection(_default_connect_info)
    sql_query = "SHOW DATABASES"

    _, data = dbutils.run_q(sql=sql_query, args=None, fetch=True, cur=None, conn=cnx, commit=True)
    if len(data) > 0:
        return list(map(lambda f: f[0], data))
    return []


def get_tables(db_name):
    global _default_connect_info
    if db_name is None:
        raise ValueError("Need to pass db name to access its tables")
    connect_info = _default_connect_info
    connect_info["db"] = db_name
    cnx = dbutils.get_connection(connect_info)
    sql_query = "SHOW TABLES IN " + db_name

    _, data = dbutils.run_q(sql=sql_query, args=None, fetch=True, cur=None, conn=cnx, commit=True)
    if len(data) > 0:
        return list(map(lambda f: f[0], data))
    return []





