"""Main module."""
from bearsql import logger
from duckdb import connect
from pandas import DataFrame
from typing import Optional, Union, Generator
import string
import random


class SqlContext:

    def __init__(self, table: Optional[str] = None, view: Optional[str] = None, database: Optional[str] = None):
        '''
        Class to create sql context for pandas dataframes so as to add
        sql syntax. 

        :param table: table name to be used. The value can be changed later
        :type table: Optional[str]; default None

        :param view: view name to be used. This value can be changed later
        :type view: Optional[str]; default None

        :param database: name of the database. A file based db will be created
                        to store all the tables. Default is :memory:. The default
                        database will persist only for a session. Post the session,
                        it will be deleted. To persist the database, we can 
                        instantiate this class by passing the database name
        :type database: str; default None
        '''
        self.db = database
        self._table = table
        self._view = view
        self.__conn = self.__engine()

    @property
    def table(self):
        '''
        Table property of the class. This will give the name of the table
        that is currently in use
        '''
        return self._table

    @table.setter
    def table(self, table: str):
        '''
        Method to set new table name
        '''
        if not table:
            raise Exception('Expected table name, got None')
        logger.info(f'Setting table name to {table}')
        self._table = table

    @property
    def view(self):
        '''
        View property of the class. This will give the name of the view
        that is currently in use
        '''
        return self._view

    @view.setter
    def view(self, view: str):
        '''
        Method to set new view name
        '''
        if not view:
            raise Exception('Expected view name, got None')
        logger.info(f'Setting view name to {view}')
        self._view = view

    def __engine(self):
        '''
        Private method to connect to duckdb

        returns: duckdb connection object
        :rtype: duckdb
        '''
        if not self.db:
            self.db = ':memory:'
        logger.info(f'Database created at {self.db}')
        return connect(self.db, read_only=False)

    def register_table(self, df: DataFrame, table: Optional[str] = None) -> None:
        '''
        This method creates a table in the database with pandas dataframe as the input. 
        To create a table, a view must be created. If there is no view name
        specified in this class, a new random view name will be generated

        :param df: pandas input dataframe
        :type df: DataFrame

        :param table: table name; the dataframe will sit in the database and 
                    can be referenced using this table name
        :type table: Optional[str]; default None
        '''
        view = self.view if self.view else ''.join(
            random.choice(string.ascii_lowercase) for i in range(10))
        self.view = view
        if table:
            self.table = table
        logger.info(f'Creating table {self.table}')
        self.register_view(df, view)
        self.__conn.execute(
            f'CREATE TABLE {self.table} as SELECT * FROM {view}')
        logger.info(f'Table {self.table} created')

    def register_view(self, df: DataFrame, view: Optional[str] = None) -> None:
        '''
        This method creates a view in the database with pandas dataframe as the input. 
        If there is no view name is passed and not specified in this class, 
        an exception will be thrown

        :param df: pandas input dataframe
        :type df: DataFrame

        :param view: table name; the dataframe will sit in the database and 
                    can be referenced using this table name
        :type view: Optional[str]; default None
        '''
        view = view if view else self.view
        if not view:
            raise Exception('No view name found! Please pass view name')
        self.view = view
        self.__conn.register(self.view, df)
        logger.info(f'View {self.view} created')

    def sql(self, query: Union[str, list], output: str = 'df') -> Generator:
        '''
        Method to run sql queries on pandas dataframe.

        :param query: sql query to execute on pandas dataframe.
                    It can be one single query or a list of
                    multiple queries
        :type query: Union[str, list]

        :param output: Output format of the query results. This can
                    either be df, arrow or any
        :type output: str; default df

        returns: Generator object containing all the query results
        :rtype: Generator
        '''
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
        '''
        Create a relational table on top of pandas dataframe. If tagged
        with a table name, this name can be used to run sql queries.

        :param df: pandas input dataframe
        :type df: DataFrame

        :param table: name of the table
        :type table: Optional[str]; default None

        returns: duckdb relation
        :rtype: duckdb
        '''
        logger.info(f'Creating relation from dataframe')
        rel = self.__conn.from_df(df)
        table = table if table else f'table_{random.randint(10,50)}'
        self.table = table
        rel.set_alias(self.table)
        logger.info(
            f'Table can now be referenced using alias {self.table}')
        return rel

    def close(self):
        '''
        Method to close database connection
        '''
        logger.info('Closing connection')
        self.__conn.close()
        logger.info('Connection closed')
