## logging ##
from pandabase.Logging import logger

## typing imports ##
from typing import Optional, Generator, List, Dict, Any
from ...DBTypes import DBCredentials, Field

## decorator imports ##
from contextlib import contextmanager

## db imports ##
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


class DirectConnection:
    '''
    Class that manages direct connections to the postgres DB
    hosted by supabase
    '''
    def __init__(self, credentials:DBCredentials):
        self.credentials:DBCredentials = credentials
        self.conn:Optional[psycopg2.extensions.connection] = None
        self.logger = logger.get_logger('db.direct')
        self.connect()

    ############################
    ## CONNECTION MANAGEMENT ##
    ############################
    def connect(self):
        '''
        Creates direct connection to the database
        '''
        try:
            self.conn = psycopg2.connect(
                host=self.credentials.db_host,
                database=self.credentials.db_name,
                user=self.credentials.db_user,
                password=self.credentials.db_password,
                port=self.credentials.db_port
            )
        except Exception as e:
            self.logger.error(
                'DB_ERROR: Failed to make direct connection to the database',
                extra={'error': str(e)}
            )
            raise e
        ## check that the connection was successful ##
        if self.conn is None:
            self.logger.error(
                'DB_ERROR: Failed to make direct connection to the database'
            )
            raise Exception('DB ERROR: Failed to make direct connection to the database')
        
    def close(self):
        '''
        Closes the connection to the database
        '''
        if self.conn:
            self.logger.info('Closing database connection') 
            self.conn.close()
    
    @contextmanager
    def session(self) -> Generator[psycopg2.extensions.connection, None, None]:
        '''
        Manage opening and closing of direct database connections to avoid
        leaving connections open and consuming db resources

        ie
        with self.session() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM information_schema.columns")
                columns = cur.fetchall()
        '''
        try:
            self.connect()
            yield self.conn
        finally:
            self.close()
    
    #############
    ## METHODS ##
    #############
    def get_table_stats(self, table_name:str)->Dict[str, Any]:
        '''
        Gets metadata for a given table in the database

        Parameters:
        * table_name : name of the table to check
        
        Returns:
        * dict of last_modified and record_count
        '''
        ## define query ##
        ## paramaterize for security ##
        query = sql.SQL('''
            SELECT 
                max(updated_at) as last_modified,
                (
                    SELECT
                        n_live_tup
                    FROM
                        pg_stat_user_tables
                    WHERE
                        relname = %s
                ) as record_count,
                (
                    SELECT
                        n_tup_ins + n_tup_upd + n_tup_del
                    FROM
                        pg_stat_user_tables
                    WHERE
                        relname = %s
                ) as total_modifications
            FROM
                {table}
        ''').format(
            table=sql.Identifier(table_name)
        )
        ## execute query ##
        try:
            self.logger.info('Getting table stats for {0}'.format(table_name))
            with self.session() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (table_name, table_name))
                    return cur.fetchone()
        except Exception as e:
            self.logger.error(
                'Failed to get table stats for {0}'.format(table_name),
                extra={'error': str(e)}
            )
            raise e


    def get_table_fields(self, table_name:str)->List[Field]:
        '''
        Gets fields for a given table in the database

        Parameters:
        * table_name: Name of the table in the database

        Returns:
        * fields: List of Field objects
        '''
        ## define query ##
        query = '''
            SELECT
                c.column_name,
                c.udt_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                STRING_AGG(tc.constraint_type, ', ') AS constraint_type
            FROM
                information_schema.columns c
                LEFT JOIN information_schema.key_column_usage kcu ON c.table_name = kcu.table_name
                AND c.column_name = kcu.column_name
                LEFT JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
            WHERE
                c.table_name = %s
            GROUP BY
                c.column_name,
                c.udt_name,
                c.data_type,
                c.is_nullable,
                c.column_default
        '''
        ## execute query ##
        fields = []
        try:
            self.logger.info('Getting table fields for {0}'.format(table_name))
            with self.session() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (table_name,))
                    field_records = cur.fetchall()
                    ## error handle ##
                    if len(field_records) == 0:
                        raise Exception('DB ERROR: Table {1} not found'.format(table_name))
                    ## convert records into Fields ##
                    for field_record in field_records:
                        ## convert record into a Field
                        field = Field(
                            name = field_record['column_name'],
                            udt_name = field_record['udt_name'],
                            data_type = field_record['data_type'],
                            is_nullable = True if field_record['is_nullable'] == 'YES' else False,
                            is_primary_key = False if field_record['constraint_type'] is None else True if 'PRIMARY KEY' in field_record['constraint_type'] else False,
                            has_unique_constraint = False if field_record['constraint_type'] is None else True if 'UNIQUE' in field_record['constraint_type'] else False,
                            has_foreign_key = False if field_record['constraint_type'] is None else True if 'FOREIGN KEY' in field_record['constraint_type'] else False,
                            has_default = True if field_record['column_default'] is not None else False,
                            default_value = field_record['column_default']
                        )
                        fields.append(field)
            return fields
        except Exception as e:
            self.logger.error(
                'Failed to get table fields for {0}'.format(table_name),
                extra={'error': str(e)}
            )
            raise e