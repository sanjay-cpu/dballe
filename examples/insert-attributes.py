#!/usr/bin/env python3
import unittest
from datetime import datetime

import dballe


class TestInsertAttributes(unittest.TestCase):
    def setUp(self):
        db = dballe.DB.connect("sqlite://:memory:")
        db.reset()
        with db.transaction() as tr:
            tr.insert_data({
                "lon": 1212345,
                "lat": 4312345,
                "report": "test",
                "datetime": datetime(2019, 1, 2, 3, 4, 5),
                "B12101": 0,
                "level": (103, 2000),
                "trange": (254, 0, 0),
            }, can_add_stations=True)

        self.db = db

    def tearDown(self):
        self.db.reset()

    def test_insert_attrs(self):
        # Insert attributes using CursorDataDB.insert_attrs
        with self.db.transaction() as tr:
            for data in tr.query_data():
                data.insert_attrs({"B33196": 1})

        attrs = []
        with self.db.transaction() as tr:
            for data in tr.query_data({"query": "attrs"}):
                for a in data["variable"].get_attrs():
                    attrs.append(a)

        self.assertEqual(attrs, [dballe.var("B33196", 1)])

    def test_insert_attrs_context(self):
        # Insert attributes using context_id
        data_to_modify = []
        with self.db.transaction() as tr:
            for data in tr.query_data():
                data_to_modify.append(data["context_id"])

        with self.db.transaction() as tr:
            for ctx in data_to_modify:
                tr.attr_insert_data(ctx, {"B33196": 1})

        attrs = []
        with self.db.transaction() as tr:
            for data in tr.query_data({"query": "attrs"}):
                for a in data["variable"].get_attrs():
                    attrs.append(a)

        self.assertEqual(attrs, [dballe.var("B33196", 1)])


if __name__ == '__main__':
    unittest.main()
