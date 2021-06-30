"""Main module."""
from bearsql import logger
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
        logger.info(f'Setting table name to {table}')
        self._table = table

    @property
    def view(self):
        return self._view

    @view.setter
    def view(self, view: str):
        if not view:
            raise Exception('Expected view name, got None')
        logger.info(f'Setting view name to {view}')
        self._view = view

    def __engine(self):
        if not self.db:
            self.db = ':memory:'
        logger.info(f'Database created at {self.db}')
        return connect(self.db, read_only=False)

    def register_table(self, df: DataFrame, table: Optional[str] = None):
        view = self.view if bool(self.view) else ''.join(
            random.choice(string.ascii_lowercase) for i in range(10))
        self.view = view
        if table:
            self.table = table
        logger.info(f'Creating table {self.table}')
        self.__conn.register(view, df)
        self.__conn.execute(
            f'CREATE TABLE {self.table} as SELECT * FROM {view}')
        logger.info(f'Table {self.table} created')

    def register_view(self, df: DataFrame, view: Optional[str] = None):
        view = self.view if bool(self.view) else ''.join(
            random.choice(string.ascii_lowercase) for i in range(10))
        self.view = view
        self.__conn.register(self.view, df)
        logger.info(f'View {self.view} created')

    def sql(self, query: list, output: str = 'df'):
        logger.info(f'Starting query execution')
        if isinstance(query, str):
            query = [query]
        for sql in query:
            logger.info(f'[EXECUTING] {sql}')
            self.__conn.execute(sql)
            if output == 'df':
                logger.info(f'Fetching output in dataframe format')
                yield self.__conn.fetch_df()
            if output == 'arrow':
                yield self.__conn.fetch_arrow_table()
            else:
                yield self.__conn.fetchall()

    def relation(self, df: DataFrame, table: Optional[str] = None):
        logger.info(f'Creating relation from dataframe')
        rel = self.__conn.from_df(df)
        if table:
            self.table = table
            rel.set_alias(self.table)
            logger.info(
                f'Table can now be referenced using alias {self.table}')
        return rel

    def close(self):
        logger.info('Closing connection')
        self.__conn.close()
        logger.info('Connection closed')
