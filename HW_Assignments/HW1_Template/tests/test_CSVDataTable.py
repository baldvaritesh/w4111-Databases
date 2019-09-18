from unittest import TestCase, mock

from parameterized import parameterized

from src.CSVDataTable import CSVDataTable
from tests.unit_tests import logger_load, data_dir, t_load


# Testing Methodology Used
# 1. For Find Methods: Tested on testing CSV data
# 2. For Load & Save Methods: Tested on actual CSV data
# 3. For other methods, created a mocked version of tests to check various scenarios
# 4. Mocking allows to test the functions without altering the state of true data
# 5. You need the People.csv in the sub-directory named Testing under Data for the save method
#    and the find methods
class TestCSVDataTable(TestCase):

    @classmethod
    def setUpClass(cls):
        # Exposing the parent class to add the logger into it for easy access
        cls._logger = logger_load()
        cls._csv_table = t_load(CSVDataTable)

    @parameterized.expand([
        ({
            "directory": data_dir("../Data/Baseball"),
            "file_name": "People.csv",
         }, None),
        ({
            "directory": data_dir("../Data/Baseball"),
            "file_name": "People.csv"
         }, ["playerID"])
    ])
    def test__init(self, connect_info, key_columns):
        csv_tbl = CSVDataTable("people", connect_info, key_columns)
        self.assertEqual(csv_tbl._data["key_columns"], key_columns)
        self.assertEqual(len(csv_tbl.get_rows()), 19617)
        print("Test: Loads People.csv with correct data, key: " + str(key_columns))

    @parameterized.expand([
        (["playerID"], [""], "Throws exception when empty string is sent"),
        (["playerID"], [None], "Throws exception when None is sent"),
        (["playerID", "retroID"], ["adamecr01", ""], "None of the keys should be empty or None")
    ])
    def test_check_invalid_primary_keys(self, key_columns, key_fields, description):
        with self.assertRaises(ValueError):
            with mock.patch.dict(self._csv_table._data, {"key_columns": key_columns}):
                self._csv_table.check_invalid_primary_keys(self._csv_table, key_fields)
        print("Test: " + description)

    @parameterized.expand([
        (["adamecr01"], ["birthYear"], {"birthYear": "1991"}, "Finds one field given correct key"),
        (["adairji01"], ["birthYear", "birthState"], {"birthYear": "1907", "birthState": "TX"},
         "Finds multiple fields given correct key"),
        (["aaairji01"], ["birthYear"], None, "Returns None if the object is not found"),
        (["adamecr01"], None, 24, "Returns all of the object when key_fields are sent")
    ])
    def test_find_by_primary_key(self, key_fields, field_list, expected_output, description):
        obtained_output = self._csv_table.find_by_primary_key(key_fields, field_list)
        if isinstance(expected_output, int):
            self.assertEqual(len(obtained_output), expected_output)
        else:
            self.assertEqual(obtained_output, expected_output)
        print("Test: " + description)

    @parameterized.expand([
        ({"birthYear": "1991"}, ["birthState"], 215, "Finds 1 field for one key template cases"),
        ({"birthYear": "1991", "birthState": "TX"}, ["birthMonth", "birthDay"], 16,
         "Finds multiple fields for multiple key templates"),
        (None, ["birthYear"], 19617, "Matches everything for empty template"),
        ({"deathYear": "", "deathMonth": ""}, ["deathDay"], 9968, "Finds the count of missing elements too"),
        ({"birthYear": "1991", "birthState": "TX"}, None, 16, "Returns all fields when field_list empty", 24)
    ])
    def test_find_by_template(self, template, field_list, expected_output, description, length=None):
        obtained_output = self._csv_table.find_by_template(template, field_list)
        self.assertEqual(len(obtained_output), expected_output)
        if length is not None:
            self.assertEqual(len(obtained_output[0]), length)
        print("Test: " + description)

    @parameterized.expand([
        ([], ["a", "b"], {"a": "1", "b": "1"}, "Adds the row to an empty list"),
        ([{"a": "1", "b": "1", "c": "1"}], ["a"], {"a": "3"}, "Fills the missing keys with empty string"),
        ([{"a": "1", "b": "2", "c": "3"}], ["a"], {"a": "4", "b": "5"}, "Performing a mixed insert")
    ])
    def test_insert(self, rows, key_columns, row, description):
        initial_length = len(rows)
        with mock.patch.multiple(self._csv_table, _rows=rows, _data={"key_columns": key_columns},
                                 save=lambda *args: None):
            self._csv_table.insert(row)
            self.assertEqual(len(self._csv_table._rows), initial_length + 1)
            # assert for inserted row length
            if not rows:
                self.assertEqual(len(self._csv_table._rows[initial_length]), len(rows[0]))
        print("Test: " + description)

    @parameterized.expand([
        (["playerID"], {"playerID": "adamecr01"}, ["adamecr01"], True, "Finds record if the primary key exists"),
        (["playerID", "retroID"], {"playerID": "aaronha01", "retroID": "aaroh101"}, ["aaronha01", "aaroh101"],
         True, "Finds record for multiple primary keys"),
        (["playerID", "retroID"], {"playerID": "aaronha01", "retroID": "aaroh101"}, ["aaronha01", "aaroh1011"],
         False, "Returns False record if primary keys do not match"),
    ])
    def test_matches_primary_key(self, key_columns, row, key_fields, expected_output, description):
        with mock.patch.dict(self._csv_table._data, {"key_columns": key_columns}):
            obtained_output = self._csv_table.matches_primary_key(self._csv_table, row, key_fields)
            self.assertEqual(obtained_output, expected_output)
        print("Test: " + description)

    @parameterized.expand([
        ({"a": 1, "b": 2, "c": 3}, {"a": 1, "c": 3}, True, "Returns True in case template is found"),
        ({"a": 1, "b": 2, "c": 3}, {"d": 4}, False, "Returns False in case any value of template is not found"),
        ({"a": 1, "b": 2, "c": 3}, {"a": 2}, False, "Returns False in case any value of template does not match")
    ])
    def test_matches_template(self, row, template, expected_output, description):
        obtained_output = self._csv_table.matches_template(row, template)
        self.assertEqual(obtained_output, expected_output)
        print("Test: " + description)

    @parameterized.expand([
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a"], ["1"], 1,
         "Deletes the element if it exists (single)"),
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a", "b"], ["1", "1"], 1,
         "Deletes the element if it exists (multiple)"),
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a"], ["0"], 0,
         "Can't delete if element not found (single)"),
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a", "b"], ["0", "1"], 0,
         "Can't delete if element not found (multiple)")
    ])
    def test_delete_by_key(self, rows, key_columns, key_fields, expected_output, description):
        with mock.patch.multiple(self._csv_table, _rows=rows, _data={"key_columns": key_columns},
                                 save=lambda *args: None):
            original_size = len(rows)
            obtained_output = self._csv_table.delete_by_key(key_fields)
            self.assertEqual(obtained_output, expected_output)
            self.assertEqual(len(self._csv_table._rows), original_size - obtained_output)
        print("Tests: " + description)

    @parameterized.expand([
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], {"a": "1"}, 1,
         "Deletes the element if it exists (single template)"),
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], {"a": "1", "b": "1"}, 1,
         "Deletes the element if it exists (multiple template)"),
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], {"a": "0"}, 0,
         "Can't delete if element not found (single template)"),
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], {"a": "1", "b": "0"}, 0,
         "Can't delete if element not found (multiple template)"),
        ([{"a": "1", "b": "1"}, {"a": "1", "b": "2"}, {"a": "2", "b": "1"}], {"a": "1"}, 2,
         "Deletes the element if it exists (multiple deletions)"),
    ])
    def test_delete_by_template(self, rows, template, expected_output, description):
        with mock.patch.multiple(self._csv_table, _rows=rows, save=lambda *args: None):
            original_size = len(rows)
            obtained_output = self._csv_table.delete_by_template(template)
            self.assertEqual(obtained_output, expected_output)
            self.assertEqual(len(self._csv_table._rows), original_size - obtained_output)
        print("Tests: " + description)

    @parameterized.expand([
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a"], ["1"], {"a": "3"},
         1, {"a": "3", "b": "1"}, "Updates the element if it exists (single)"),
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a", "b"], ["1", "1"], {"a": "3", "b": "3"},
         1, {"a": "3", "b": "3"}, "Updates the element if it exists (multiple)"),
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a", "b"], ["0", "1"], {"a": "3", "b": "3"},
         0, None, "Can't update if element not found (multiple)")
    ])
    def test_update_by_key(self, rows, key_columns, key_fields, new_values, expected_output,
                           expected_element, description):
        with mock.patch.multiple(self._csv_table, _rows=rows, _data={"key_columns": key_columns},
                                 save=lambda *args: None):
            obtained_output = self._csv_table.update_by_key(key_fields, new_values)
            self.assertEqual(obtained_output, expected_output)
            if obtained_output:
                obtained_element = self._csv_table.find_by_template(new_values)[0]
                self.assertEqual(obtained_element, expected_element)
        print("Tests: " + description)

    @parameterized.expand([
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], {"a": "1"}, {"a": "3", "b": "2"},
         1, [{"a": "3", "b": "2"}], "Updates the element if it exists (single)"),
        ([{"a": "1", "b": "1"}, {"a": "1", "b": "2"}, {"a": "2", "b": "2"}], {"a": "1"}, {"a": "3"},
         2, [{"a": "3", "b": "1"}, {"a": "3", "b": "2"}], "Updates the elements if they exists (multiple)"),
        ([{"a": "1", "b": "1"}, {"a": "1", "b": "2"}, {"a": "2", "b": "2"}], {"a": "1", "b": "1"}, {"a": "3"},
         1, [{"a": "3", "b": "1"}], "Updates the element if it exists (multiple template)")
    ])
    def test_update_by_template(self, rows, template, new_values, expected_output, expected_element, description):
        with mock.patch.multiple(self._csv_table, _rows=rows, save=lambda *args: None, _data={"key_columns": None}):
            obtained_output = self._csv_table.update_by_template(template, new_values)
            self.assertEqual(obtained_output, expected_output)
            if obtained_output:
                obtained_element = self._csv_table.find_by_template(new_values)
                self.assertEqual(obtained_element, expected_element)
        print("Tests: " + description)

    @parameterized.expand([
        ({
             "directory": data_dir("../Data/Baseball/Testing"),
             "file_name": "People.csv",
         }, None),
    ])
    def test_save(self, connect_info, key_columns):
        csv_tbl = CSVDataTable("people", connect_info, key_columns)
        # Change something
        changes = csv_tbl.update_by_template({"playerID": "aardsda01"}, {"birthYear": "1990"})
        self.assertEqual(changes, 1)
        # Assert the that it has been written
        csv_tbl2 = CSVDataTable("people2", connect_info, key_columns)
        changed_player = csv_tbl2.find_by_template({"playerID": "aardsda01"})
        self.assertEqual(changed_player[0]["birthYear"], "1990")
        # Revert the change
        changes = csv_tbl.update_by_template({"playerID": "aardsda01"}, {"birthYear": "1981"})
        self.assertEqual(changes, 1)
        print("Test: Saves People.csv with correct data")

    @parameterized.expand([
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a"], ["0"], {"a": "1"}, "key",
         "Can't update if element new value already exists"),
        ([{"a": "1", "b": "1", "c": "1"}, {"a": "1", "b": "2", "c": "2"}], ["a", "b"], {"c": "2"},
         {"b": "1"}, "template", "Can't update if element new value already exists: template"),
    ])
    def test_update_with_existing_key(self, rows, key_columns, key_fields, new_values, method, description):
        with mock.patch.multiple(self._csv_table, _rows=rows, _data={"key_columns": key_columns},
                                 save=lambda *args: None):
            with self.assertRaises(ValueError):
                if method == "key":
                    self._csv_table.update_by_key(key_fields, new_values)
                else:
                    self._csv_table.update_by_template(key_fields, new_values)
        print("Tests: " + description)

    @parameterized.expand([
        ([{"a": "1", "b": "1"}, {"a": "2", "b": "2"}], ["a"], {"a": "1", "b": "2"},
         "Can't insert element if element with same key exists"),
    ])
    def test_insert_with_existing_key(self, rows, key_columns, new_record, description):
        with mock.patch.multiple(self._csv_table, _rows=rows, _data={"key_columns": key_columns},
                                 save=lambda *args: None):
            with self.assertRaises(ValueError):
                self._csv_table.insert(new_record)
        print("Tests: " + description)
