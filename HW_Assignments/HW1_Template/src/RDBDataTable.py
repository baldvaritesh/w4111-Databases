import pymysql

from src.BaseDataTable import BaseDataTable


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
        }

        # Maintain a cursor to the connection
        self._conn = self._load()

    def _load(self):
        connection = pymysql.connect(host=self._data["connect_info"]["host"],
                                     user=self._data["connect_info"]["user"],
                                     password=self._data["connect_info"]["password"],
                                     db=self._data["connect_info"]["db"],
                                     charset="utf8mb4",
                                     cursorclass=pymysql.cursors.DictCursor)

        return connection

    def _run_sql_query(self, q, fetch=True, cnx=None):
        """

        :param q: The basic SQL statement
        :param fetch:
        :param cnx:
        :return:
        """
        r = None
        try:
            cnx = self._conn

            cursor = cnx.cursor()
            r = cursor.execute(q)  # Execute the query.

            if fetch:
                r = cursor.fetchall()  # Return all elements of the result.
                if r == ():
                    r = None
        except Exception as e:
            print("Exception e = ", e)

        # Ideally should only be for insert, update, delete
        cnx.commit()

        return r

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        sql_stmt = "SELECT {} FROM {}".format(",".join(field_list if field_list is not None else ["*"]),
                                              self._data["connect_info"]["db"] + "." + self._data["table_name"])
        sql_stmt += " WHERE " + " AND ".join("%s='%s'" % (k, v) for k, v in zip(self._data["key_columns"], key_fields))
        sql_stmt += ";"

        return self._run_sql_query(sql_stmt)

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        sql_stmt = "SELECT {} FROM {}".format(",".join(field_list if field_list is not None else ["*"]),
                                              self._data["connect_info"]["db"] + "." + self._data["table_name"])
        sql_stmt += " WHERE " + " AND ".join("%s='%s'" % (k, template[k]) for k in template)
        sql_stmt += ";"

        return self._run_sql_query(sql_stmt)

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        sql_stmt = "DELETE FROM {}".format(self._data["connect_info"]["db"] + "." + self._data["table_name"])
        sql_stmt += " WHERE " + " AND ".join("%s='%s'" % (k, v) for k, v in zip(self._data["key_columns"], key_fields))
        sql_stmt += ";"

        return self._run_sql_query(sql_stmt, fetch=False)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        sql_stmt = "DELETE FROM {}".format(self._data["connect_info"]["db"] + "." + self._data["table_name"])
        sql_stmt += " WHERE " + " AND ".join("%s='%s'" % (k, template[k]) for k in template)
        sql_stmt += ";"

        return self._run_sql_query(sql_stmt, fetch=False)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        sql_stmt = "UPDATE {} SET ".format(self._data["connect_info"]["db"] + "." + self._data["table_name"])
        sql_stmt += ", ".join("%s='%s'" % (k, new_values[k]) for k in new_values)
        sql_stmt += " WHERE " + " AND ".join("%s='%s'" % (k, v) for k, v in zip(self._data["key_columns"], key_fields))

        return self._run_sql_query(sql_stmt, fetch=False)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        sql_stmt = "UPDATE {} SET ".format(self._data["connect_info"]["db"] + "." + self._data["table_name"])
        sql_stmt += ", ".join("%s='%s'" % (k, new_values[k]) for k in new_values)
        sql_stmt += " WHERE " + " AND ".join("%s='%s'" % (k, template[k]) for k in template)

        return self._run_sql_query(sql_stmt, fetch=False)

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        sql_stmt = "INSERT INTO {} (".format(self._data["connect_info"]["db"] + "." + self._data["table_name"])
        sql_stmt += ", ".join(new_record.keys())
        sql_stmt += ") VALUES ("
        sql_stmt += ", ".join(map(lambda x: "'" + x + "'", new_record.values()))
        sql_stmt += ");"

        self._run_sql_query(sql_stmt, fetch=False)

    def get_rows(self):
        return self._rows




