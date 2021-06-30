from duckdb import connect
from pandas import DataFrame
from typing import Optional
import string
import random


class SqlContext:

    def __init__(self, table: Optional[str] = None, view: Optional[str] = None, database: str = None):
        self.db = database
        self._table = table
        self._view = view
        self.__conn = self.__engine()

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, table: str):
        if not table:
            raise Exception('Expected table name, got None')
        self._table = table

    @property
    def view(self):
        return self._view

    @view.setter
    def view(self, view: str):
        if not view:
            raise Exception('Expected table name, got None')
        self._view = view

    def __engine(self):
        if not self.db:
            self.db = ':memory:'
        return connect(self.db, read_only=False)

    def register_table(self, df: DataFrame, table: Optional[str] = None):
        view = self.view if self.view else ''.join(
            random.choice(string.ascii_lowercase) for i in range(10))
        if table:
            self.table = table
        self.__conn.register(view, df)
        self.__conn.execute(
            f'CREATE TABLE {self.table} as SELECT * FROM {view}')

    def register_view(self, df: DataFrame, view: Optional[str] = None):
        if view:
            self.view = view
        self.__conn.register(self.view, df)

    def sql(self, query: list, output: str = 'df'):
        if isinstance(query, str):
            query = [query]
        for sql in query:
            self.__conn.execute(sql)
            if output == 'df':
                yield self.__conn.fetch_df()
            if output == 'arrow':
                yield self.__conn.fetch_arrow_table()
            else:
                yield self.__conn.fetchall()

    def relation(self, df: DataFrame, table: Optional[str] = None):
        rel = self.__conn.from_df(df)
        if table:
            self.table = table
            rel.set_alias(self.table)
        return rel

    def close(self):
        self.__conn.close()
