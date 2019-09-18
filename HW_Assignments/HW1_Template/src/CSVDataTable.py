
import copy
import csv
import json
import logging
import os
from functools import reduce

import pandas as pd

from src.BaseDataTable import BaseDataTable

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)
        delimiter = ","
        if "delimiter" in self._data["connect_info"]:
            delimiter = self._data["connect_info"].get("delimiter")

        key_map = {}
        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file, delimiter=delimiter)
            for r in csv_d_rdr:
                # Check the sanity of the row being entered
                if self._data["key_columns"] is not None:
                    key = tuple([r.get(k) for k in self._data["key_columns"]])
                    if None in key:
                        raise ValueError("Key does not exist in the table")
                    if key in key_map:
                        # Throw exception
                        raise ValueError("Key already exists in the database")
                    key_map[key] = True
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "w", newline="") as txt_file:
            if len(self._rows) is not 0:
                csv_d_writer = csv.DictWriter(txt_file, fieldnames=self._rows[0].keys())
                csv_d_writer.writeheader()
                for r in self._rows:
                    csv_d_writer.writerow(r)

        self._logger.debug("CSVDataTable._save: Saved " + str(len(self._rows)) + " rows")

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    @staticmethod
    def matches_primary_key(self, row, key_fields):
        result = True

        if self._data.get("key_columns") is not None:
            for i in range(len(self._data.get("key_columns"))):
                if key_fields[i] != row.get((self._data.get("key_columns"))[i], None):
                    result = False
                    break

        return result

    @staticmethod
    def check_invalid_primary_keys(self, key_fields):
        # Check validity only when the primary keys are set for the table
        if self._data["key_columns"] is None:
            return True

        for v in key_fields:
            if v is None or v is "":
                raise ValueError("Invalid Primary Keys Provided")

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        self.check_invalid_primary_keys(self, key_fields)

        result = None
        for row in self._rows:
            if self.matches_primary_key(self, row, key_fields):
                if field_list is None:
                    result = row
                else:
                    result = dict((k, row[k]) for k in field_list if k in row)
                break
        return result

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
        result = []
        for row in self._rows:
            if self.matches_template(row, template):
                if field_list is None:
                    result.append(row)
                else:
                    result.append(dict((k, row[k]) for k in field_list if k in row))
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: List of value for the key fields.
        :return: A count of the rows deleted.
        """
        self.check_invalid_primary_keys(self, key_fields)

        count = 0
        for idx, row in enumerate(self._rows):
            if self.matches_primary_key(self, row, key_fields):
                count += 1
                del self._rows[idx]
                break

        if count > 0:
            self.save()

        return count

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        count = []
        for idx, row in enumerate(self._rows):
            if self.matches_template(row, template):
                count += [idx]

        for idx in sorted(count, reverse=True):
            del self._rows[idx]

        if len(count) > 0:
            self.save()

        return len(count)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        self.check_invalid_primary_keys(self, key_fields)

        # To Check if the user is trying to update the primary key
        new_primary_key_fields = copy.copy(key_fields)
        for k, v in enumerate(self._data["key_columns"]):
            if v in new_values:
                new_primary_key_fields[k] = new_values[v]
        new_key_element = self.find_by_primary_key(new_primary_key_fields)

        count = 0
        if new_key_element is None:
            # Allow update if it is still unique
            for idx, row in enumerate(self._rows):
                if self.matches_primary_key(self, row, key_fields):
                    count += 1
                    row.update(new_values)
                    break
        else:
            raise ValueError("The element with that key already exists. Can't update the key to this value")

        if count > 0:
            self.save()

        return count

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        idxes = []
        # Need to prevent the user from updating multiple primary keys at once
        for idx, row in enumerate(self._rows):
            if self.matches_template(row, template):
                idxes.append(idx)
                if self._data["key_columns"] is not None:
                    new_key = [new_values[k] if k in new_values else row[k] for k in self._data["key_columns"]]
                    element = self.find_by_primary_key(new_key)
                    if element is not None:
                        raise ValueError("You are trying to update keys which already exist in the table")

        for idx in idxes:
            self._rows[idx].update(new_values)

        if len(idxes) > 0:
            self.save()

        return len(idxes)

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        if self._data["key_columns"] is not None:
            # Check for invalid primary key entries
            self.check_invalid_primary_keys(self, [new_record[k] for k in self._data["key_columns"]])

        if len(self._rows) != 0:
            # 1. Check if all keys of new_record are subset of original data
            # 2. Check to see if all keys are set to match dimensionality of data
            # 3. Check if element with key doesn't already exist
            keys_original = self._rows[0].keys()
            keys_new = new_record.keys()

            if not reduce(lambda x, y: x and y, [True if t in keys_original else False for t in keys_new]):
                raise ValueError("Extra Keys present in new record which are not in table")

            element = self.find_by_primary_key([new_record[k] for k in self._data["key_columns"]])

            if element is None:
                for k in keys_original:
                    if k not in new_record:
                        new_record.update({k: ""})
            else:
                raise ValueError("Insert failed since element with duplicate key already exists")

        self._add_row(new_record)

        self.save()

    def get_rows(self):
        return self._rows

