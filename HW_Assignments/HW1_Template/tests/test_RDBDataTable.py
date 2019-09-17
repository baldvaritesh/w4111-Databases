from unittest import TestCase

from src.RDBDataTable import RDBDataTable
from tests.unit_tests import logger_load, sql_load


# Testing Methodology Used
# Most of the tests are performed by assertion
# They perform an operation on the data
# The operation is then asserted for the correctness
# The operation is reverted to bring data back to original state
class TestRDBDataTable(TestCase):

    @classmethod
    def setUpClass(cls):
        cls._logger = logger_load()
        cls._rdb_table = sql_load(RDBDataTable)

    def test_find_by_primary_key(self):
        r = self._rdb_table.find_by_primary_key(["adamecr01"], ["birthState", "birthYear"])
        print(r)

    def test_find_by_template(self):
        r = self._rdb_table.find_by_template({"birthState": "TX"}, ["playerID", "birthMonth"])
        print(r)

    def test_delete_by_key(self):
        # First find an element by key, then delete it, find it again and then insert it
        r = self._rdb_table.find_by_primary_key(["adamecr01"], None)
        r2 = self._rdb_table.delete_by_key(["adamecr01"])
        self.assertEqual(r2, 1)
        r3 = self._rdb_table.find_by_primary_key(["adamecr01"], None)
        self.assertEqual(r3, None)
        r4 = self._rdb_table.insert(r[0])
        self.assertEqual(r4, None)

    def test_delete_by_template(self):
        # First find an element by key, then delete it, find it again and then insert it
        r = self._rdb_table.find_by_template({"playerID": "adamecr01"}, None)
        r2 = self._rdb_table.delete_by_template({"playerID": "adamecr01"})
        self.assertEqual(r2, 1)
        r3 = self._rdb_table.find_by_template({"playerID": "adamecr01"}, None)
        self.assertEqual(r3, None)
        r4 = self._rdb_table.insert(r[0])
        self.assertEqual(r4, None)

    def test_update_by_key(self):
        # Try to insert your data record
        r = self._rdb_table.insert(
            {"playerID": "baldvr", "birthState": "HR", "birthYear": "1994", "birthCountry": "India"})
        self.assertEqual(r, None)
        # Try to update it now
        r2 = self._rdb_table.update_by_key(["baldvr"], {"birthYear": "1995"})
        self.assertEqual(r2, 1)
        # Check it is updated
        r3 = self._rdb_table.find_by_primary_key(["baldvr"], None)
        self.assertEqual(r3[0]["birthYear"], "1995")
        # Now remove it
        r3 = self._rdb_table.delete_by_key(["baldvr"])
        self.assertEqual(r3, 1)

    def test_update_by_template(self):
        # Try to insert your data record
        r = self._rdb_table.insert(
            {"playerID": "baldvr", "birthState": "HR", "birthYear": "1994", "birthCountry": "India"})
        self.assertEqual(r, None)
        # Try to update it now
        r2 = self._rdb_table.update_by_template({"playerID": "baldvr"}, {"birthYear": "1995"})
        self.assertEqual(r2, 1)
        # Check it is updated
        r3 = self._rdb_table.find_by_template({"playerID": "baldvr"}, None)
        self.assertEqual(r3[0]["birthYear"], "1995")
        # Now remove it
        r3 = self._rdb_table.delete_by_template({"playerID": "baldvr"})
        self.assertEqual(r3, 1)

    def test_insert(self):
        # Try to insert your data record
        r = self._rdb_table.insert(
            {"playerID": "baldvr", "birthState": "HR", "birthYear": "1994", "birthCountry": "India"})
        self.assertEqual(r, None)
        # Try to find it now
        r2 = self._rdb_table.find_by_primary_key(["baldvr"], None)
        self.assertEqual(len(r2), 1)
        self.assertEqual(r2[0]["playerID"], "baldvr")
        # Now remove it
        r3 = self._rdb_table.delete_by_key(["baldvr"])
        self.assertEqual(r3, 1)