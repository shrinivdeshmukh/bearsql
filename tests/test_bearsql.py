#!/usr/bin/env python

"""Tests for `bearsql` package."""


import unittest
import pandas as pd
import pyarrow as pa
from bearsql import SqlContext


class TestBearsql(unittest.TestCase):
    """Tests for `bearsql` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        self.df = pd.DataFrame([{'name': 'John Doe', 'city': 'New York'}, {'name': 'Jane Doe', 'city': 'Chicago'}])
        pass

    def tearDown(self):
        """Tear down test fixtures, if any."""
        pass

    def test_register_table(self):
        """Test something."""
        sc = SqlContext()
        sc.register_table(df=self.df, table='testable')
        df = sc.sql('select * from testable', output='df')
        pd.testing.assert_frame_equal(next(df), self.df)
        self.assertEqual(sc.table, 'testable')
        sc.close()

    def test_register_view(self):
        sc = SqlContext()
        sc.register_view(df=self.df, view='testview')
        df = sc.sql('select * from testview', output='df')
        pd.testing.assert_frame_equal(next(df), self.df)
        self.assertEqual(sc.view, 'testview')
        sc.close()

    def test_register_view_exception(self):
        sc = SqlContext()
        with self.assertRaises(Exception) as e:
            sc.register_view(df=self.df)
        self.assertEqual(e.exception.args, ('No view name found! Please pass view name',))
        sc.close()

    def test_table_exception(self):
        sc = SqlContext()
        with self.assertRaises(Exception) as e:
            sc.table = None
        self.assertEqual(e.exception.args, ('Expected table name, got None',))
        sc.close()

    def test_view_exception(self):
        sc = SqlContext()
        with self.assertRaises(Exception) as e:
            sc.view = None
        self.assertEqual(e.exception.args, ('Expected view name, got None',))
        sc.close()

    def test_sql_output_arrow(self):
        sc = SqlContext()
        sc.register_table(df=self.df, table='testable')
        table_generator = sc.sql('select * from testable', output='arrow')
        pd.testing.assert_frame_equal(self.df, next(table_generator).to_pandas())
        sc.close()

    def test_sql_output_tuple(self):
        sc = SqlContext()
        sc.register_table(df=self.df, table='testable')
        table_generator = sc.sql('select * from testable', output='any')
        self.assertEqual(next(table_generator), [('John Doe', 'New York'), ('Jane Doe', 'Chicago')])
        sc.close()

    def test_relation(self):
        sc = SqlContext()
        table = sc.relation(df=self.df, table='new_table')
        df = table.filter('name == \'John Doe\'').df()
        pd.testing.assert_frame_equal(self.df.where(df["name"]=="John Doe").dropna(), df)
        sc.close()

