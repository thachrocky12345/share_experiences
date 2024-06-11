#!/usr/bin/env python3

import logging
import time
import os
import psycopg2
import csv
from decimal import Decimal
from collections import namedtuple
from psycopg2 import extras, OperationalError
from psycopg2.extensions import register_adapter
from main.error import DbConnectError
from pandas.io import sql as psql
from main.config import db_config

# Set logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETLConnector")

# Constants
ON_CONFLICT_DO_NOTHING = 'ON CONFLICT DO NOTHING'
RECONNECT_ATTEMPTS = 3

# Execution types
FETCH_ONE = 'one'
FETCH_ALL = 'all'
MODIFY = 'modify'
ON_CONFLICT = ' ON CONFLICT DO NOTHING '

ExecutionResults = namedtuple('ExecutionResults', ['query_data', 'rowcount', 'cursor_description'])


current_file_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the SQL file



class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger.addHandler(NullHandler())


# Adapters
def adapt_decimal_to_float(decimal_value):
    """Converts decimal values to float; used for psycopg2 adapter."""
    return float(decimal_value) if decimal_value is not None else None


register_adapter(Decimal, adapt_decimal_to_float)


def get_query(query):
    """ gets a query by file name """
    file_name = current_file_dir + '/../_query/' + query + '.sql'
    with open(file_name, 'r') as query_file:
        sql = query_file.read()

    return sql


def get_deploy(query):
    """ gets a query by file name """
    file_name = current_file_dir + '/../_deploy/' + query + '.sql'
    with open(file_name, 'r') as query_file:
        sql = query_file.read()

    return sql


class DatabaseConnection:
    """Manage PostgreSQL database connection and operations."""

    def __init__(self, config):
        """
        Initialize the database connection.

        Parameters:
        - config (dict): Database configuration including host, port, database, user, and password.
        """
        self.config = config
        self.connection = None
        self._ensure_connection()

    def _ensure_connection(self):
        """Ensures that the database connection is established."""

        for attempt in range(RECONNECT_ATTEMPTS):
            try:
                self._connect()
                self.connection.autocommit = True
                break
            except Exception as e:
                sleep_time = 6 - attempt
                logger.warning(f'Retry in {sleep_time} seconds. Attempts left: {RECONNECT_ATTEMPTS - attempt - 1}')
                time.sleep(sleep_time)
        else:
            raise DbConnectError('Failed to connect to MatchDB.')

    def _connect(self):
        """Establishes connection to the database."""
        try:
            self.connection = psycopg2.connect(**self.config)
        except OperationalError as error:
            logger.error(f"Failed to connect to the database: {error}")
            raise

    def execute_row(self, sql, *args, **kwargs):
        """
        Execute with parameter
        :param sql: query (str): SQL query to be executed.
        :param args: params list or tuple: Parameters to pass to the SQL query.
        :param kwargs: params (dict): Parameters to pass to the SQL query.
        :return: A list of tuples (if fetch='all'), a single tuple (if fetch='one'), or None
        """
        if kwargs:
            args = kwargs
        else:
            args = args

        with self.connection.cursor(cursor_factory=extras.DictCursor) as cursor:
            cursor.execute(sql, args)

    def execute_query(self, query, params=None, fetch='all', dict_cursor=False):
        """
        Executes a SQL query and fetches results.

        Parameters:
        - query (str): SQL query to be executed.
        - params (tuple or dict, optional): Parameters to pass to the SQL query.
        - fetch (str, optional): Determines how to fetch results ('all', 'one', or None).

        Returns:
        - A list of tuples (if fetch='all'), a single tuple (if fetch='one'), or None.
        """
        if self.connection.closed != 0:
            self._ensure_connection()

        if dict_cursor is False:
            cursor_type = extras.NamedTupleCursor
        else:
            cursor_type = extras.RealDictCursor

        results = ExecutionResults(
            query_data=[],
            rowcount=0,
            cursor_description=None
        )

        with self.connection.cursor(cursor_factory=cursor_type) as cursor:
            try:
                cursor.execute(query, params)

                if fetch == FETCH_ONE:
                    query_data = cursor.fetchone()
                elif fetch == FETCH_ALL:
                    query_data = cursor.fetchall()
                else:
                    query_data = None

                results = ExecutionResults(
                    query_data=query_data,
                    rowcount=cursor.rowcount,
                    cursor_description=cursor.description if fetch == MODIFY else None
                )
            except Exception as error:

                if params and "%" in query:
                    try:
                        logger.debug("""sql to be executed: {}""".format(query % (params)))
                    except:
                        pass
                else:
                    logger.debug("""sql to be executed: {}""".format(query))

                logger.error(f"{fetch}: {query}-{error}")

            return results

    def fetch_one_row(self, sql, args=None, dict_cursor=False):
        """
        Execute a select statement and fetch a single row.
        """
        return self.execute_query(sql, args, FETCH_ONE, dict_cursor=dict_cursor)

    def fetch_all_rows(self, sql, args=None, dict_cursor=False):
        """
        Execute a select statement and fetch all rows
        """
        return self.execute_query(sql, args, FETCH_ALL, dict_cursor=dict_cursor)

    def modify_rows(self, sql, args=None):
        """
        Execute an insert, update or delete statement.
        """

        return self.execute_query(sql, args, MODIFY)

    def insert_data(self, table, data, return_id=False):
        """
        Inserts data into a table.

        Parameters:
        - table (str): Table name.
        - data (dict or list of dicts): Data to insert.
        - return_id (bool, optional): Whether to return the ID of the inserted row.

        Returns:
        - The ID of the last inserted row if return_id is True, otherwise None.
        """
        columns = data.keys() if isinstance(data, dict) else data[0].keys()
        column_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table} ({column_str}) VALUES ({placeholders}) {ON_CONFLICT_DO_NOTHING}"

        if return_id:
            query += " RETURNING id"

        params = tuple(data.values()) if isinstance(data, dict) else [tuple(d.values()) for d in data]
        return self.execute_query(query, params, fetch='one' if return_id else None)


    def streaming_cursor(self, sql, args=None):
        """
        Generator function that executes a server side cursor.
        Minimize the burden of fetchall in a query that might return a large volume

        :param cursor_name: A string representing the name passed to the server side cursor
        :param sql: A string representing the sql statment to be executed
        :param args: A dictionary or sequence representing the arguments passed to the sql statement
        """

        with self.connection as cxn:
            with cxn.cursor() as cursor:
                cursor.arraysize = 3000
                logger.debug(cursor.mogrify(sql, args))
                cursor.execute(sql, args)
                while True:
                    result_set = cursor.fetchmany()
                    if not result_set:
                        break
                    for row in result_set:
                        yield row

    def close(self):
        """Closes the database connection."""
        if self.connection:
            self.connection.close()

    def get_dataframe(self, sql, args=None):
        """
        This will be used in machine learning to generate dataframe data for statistical report
        :param sql:
        :param args:
        :return:
        """
        logger.debug("""executing cursor to dataframe""")
        if args:
            logger.debug("""sql to be executed: {}""".format(sql%(args)))
        else:
            logger.debug("""sql to be executed: {}""".format(sql))

        return psql.read_sql(sql, con=self.connection, params=args)


class DatabaseOperationError(Exception):
    """Custom exception for database operation errors."""


class InsertBlock:
    """
    Base class for executing SQL operations with dynamic data.

    Attributes:
        db: Database connection object.
        sql_template: SQL template for the operation.
        data: Data to be inserted or updated.
        return_id: Flag indicating whether to return the ID of affected rows.
    """
    _sql = None
    _values = None

    def __init__(self, db, header, sql_template, data, return_id=False):
        """
        Initializes the database block with necessary parameters.

        Args:
            db: Database connection object.
            header: insert or update statement example:

            '''insert into table(col1,col2)'''

            sql_template: SQL template string. Example:

            '''({} , {})''' or '''({col1} , {col2})'''

            data: Data for the SQL operation, as a list of dicts or tuples.
            return_id: Whether to return the IDs of inserted/updated rows.
        """
        self.db = db
        self.sql_template = sql_template
        self.data = data
        self.header = header
        self.return_id = return_id
        self.inserted_count = 0

    @property
    def values(self):
        self._values = []

        if self.is_dict() is True:
            for instance in self.data:
                self._values.append(self.sql_template.format(**instance))
        else:
            for instance in self.data:
                self._values.append(self.sql_template.format(*instance))

        self._values = ",".join(self._values)

        self._values = '''{}'''.format(self._values)

        return self._values

    @property
    def sql(self):
        sql = self.header + ' values ' + self.values + ON_CONFLICT

        if self.return_id:
            sql += " RETURNING ID"

        return sql

    @property
    def set_statement(self):
        raise NotImplementedError

    def execute(self):
        inserted_count = 0
        if self.return_id is True:
            inserted_count = self.db.fetch_all_rows(self.sql).rowcount
        else:
            self.db.modify_rows(self.sql)

        logger.debug(f"Executing SQL: {self.sql}")
        return inserted_count

    def is_dict(self):
        if isinstance(self.data[0], dict):
            is_dict = True
        elif isinstance(self.data[0], list):
            is_dict = False
        else:
            raise ValueError("Either dict or list only")
        return is_dict


class BlockList(InsertBlock):
    """
    This is more dynamic using template %s instead of {}
    example:
    header = '''Insert into test(col1, col2)'''
    template = ''' (%s, %s)'''
    """
    def __init__(self, db, header, template, data, return_id=False):
        InsertBlock.__init__(self, db, header, template, data, return_id=return_id)

    @property
    def values(self):
        temp_values = [self.sql_template] * len(self.data)
        return ",".join(temp_values)

    @property
    def args(self):
        ret = []
        for row in self.data:
            ret += row
        return ret

    def execute(self):
        inserted_count = 0
        if self.return_id:
            inserted_count = self.db.fetch_all_rows(self.sql, args=self.args).rowcount
        else:
            self.db.execute_row(self.sql, *self.args)
        return inserted_count

class BulkDb(object):

    _divider = None

    def __init__(self, db, inserted_count=True):
        """
        Initialize the BulkDb object with a database connection.

        :param db: Database connection object.
        """
        self.db = db
        self.inserted_count = inserted_count

    def insert_dynamic(self, header: str, template: str, data: list, block_size: int = 3000):
        """
        Performs bulk inserts of data into a database in specified block sizes.

        :param header: The SQL insert statement header, e.g., "INSERT INTO test(col1, col2)".
        :param template: The SQL value template for the insert statement, e.g., "(%s, %s)".
        :param data: A list of tuples, where each tuple corresponds to a row of data to be inserted.
        :param block_size: The number of rows to insert in each transaction block. Default is 3000.

        Example usage:
            header = "INSERT INTO test(col1, col2)"
            template = "(%s, %s)"
            bulk_insert = BulkDb(db=db_connection)
            bulk_insert.insert_dynamic(header=header, template=template, data=list_of_data, block_size=10)
        """
        if not isinstance(data, list):
            raise ValueError("Data must be a list")
        total_inserted = 0
        total_rows = len(data)
        for start in range(0, total_rows, block_size):
            end = min(start + block_size, total_rows)
            block_data = data[start:end]
            if block_data:
                exec_block = BlockList(db=self.db, header=header, template=template, data=block_data,
                                       return_id=self.inserted_count)
                total_inserted += exec_block.execute()
                print("total_inserted", total_inserted)

        return total_inserted

    def insert_cast(self, header: str, template: str, data: list, block_size: int = 3000):
        """
        Performs bulk inserts of data into a database in specified block sizes.

        :param header: The SQL insert statement header, e.g., "INSERT INTO test(col1, col2)".
        :param template: The SQL value template for the insert statement, e.g., "(%s, %s)".
        :param data: A list of tuples, where each tuple corresponds to a row of data to be inserted.
        :param block_size: The number of rows to insert in each transaction block. Default is 3000.

        Example usage:
            header = "INSERT INTO test(col1, col2)"
            template = ''' ({}::Integer, '{}'::timestamp)'''
            or ''' ({col1}, {col2})'''
            bulk_insert = BulkDb(db=db_connection)
            bulk_insert.insert(header=header, template=template, data=list_of_data, block_size=10)
        """
        assert isinstance(data, list), "Data must be a list"

        if not isinstance(data, list):
            raise ValueError("Data must be a list")

        total_rows = len(data)
        total_inserted = 0
        for start in range(0, total_rows, block_size):
            end = min(start + block_size, total_rows)
            block_data = data[start:end]
            if block_data:
                exec_block = InsertBlock(db=self.db, header=header,
                                         sql_template=template, data=block_data, return_id=self.inserted_count)
                total_inserted += exec_block.execute()
                # print("total_inserted", total_inserted)

        return total_inserted


def load_csv(file_path, ignore_header=True):
    data = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)

        # Optionally, skip the header if there is one
        if ignore_header:
            next(csv_reader, None)  # This skips the first row

        for row in csv_reader:
            row = [i if i else None for i in row]
            data.append(row)
    return data

# Example Usage
if __name__ == '__main__':


    db_conn = DatabaseConnection(db_config)

    data = {
        "id": 1,
        "email": "test@email.com",
        "date_of_birth": '2024-01-01',
        'is_active': True,
        "phone_number": '134-345-1234',
        'first_name': "thach",
        "last_name": "Bui",
        "postal_code": '64118'
    }

    # sql = get_deploy("client_table")
    # print(sql)
    # db_conn.modify_rows(sql)

    data = {

        "table_name": "data.client",
        "tag": 'Insert extracted client data',
        'file_name': "client_data_100k.csv",
        "status": 'extracted'
    }



    # print(db_conn.insert_data("etl.job_history", data, return_id=True))
    #
    # results = db_conn.fetch_all_rows("SELECT * FROM etl.job_history")
    # for row in results.query_data:
    #     print(row)


    # file_path = "../_data/import/client_data.csv"
    # load_data = load_csv(file_path)
    # data_insert = []
    # data_l = []
    #
    # for row in load_data:
    #     # print(row)
    #     data_insert.append(
    #         {
    #             "id": row[0],
    #             "email": row[1],
    #             "date_of_birth": row[2] if row[2] else '2022-01-01',
    #             'is_active': bool(row[3]),
    #             "phone_number": row[4],
    #             'first_name': row[5],
    #             "last_name": row[6],
    #             "postal_code": str(row[7])
    #         }
    #     )
    #     row = [i if i else None for i in row]
    #     data_l.append(row)
    #     # break
    # #
    # bulk = BulkDb(db_conn)
    # ret = bulk.insert_cast(
    #     header="insert into data.client",
    #     template="({id}, '{email}', '{date_of_birth}'::date, '{is_active}'::boolean, '{phone_number}'"
    #              ", '{first_name}', '{last_name}', '{postal_code}')",
    #     data=data_insert,
    #     block_size=2000
    # )
    # ret = bulk.insert_dynamic(header="insert into data.client",
    #                             template="(%s, %s, %s, %s,%s, %s,%s, %s)",
    #                             data=data_l)
    # print(ret)
    # #
    # sql = get_query("get_client_data")
    # print(sql)
    # data = db_conn.fetch_all_rows(sql)
    # print(data.rowcount, data.query_data[0] if data.rowcount else None)


    # # Example insert
    # data_to_insert = {'column1': 'value1', 'column2': 'value2'}
    # db_conn.insert_data('mytable', data_to_insert)
    #
    # # Fetch data
    # results = db_conn.execute_query("SELECT * FROM mytable WHERE column1 = %s", params=('value1',), fetch='all')
    # for row in results:
    #     print(dict(row))

    # db_conn.close()
