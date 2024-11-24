## system imports ##
import os
import pathlib

## data imports ##
import pandas as pd

## logging ##
from pandabase.Logging import logger

## typing imports ##
from typing import Optional, Generator, List, Dict, Any
from ..DBTypes import Field, DBCredentials

## decorator imports ##
from contextlib import contextmanager

## db imports ##
from dotenv import load_dotenv
import psycopg2

## local imports ##
from .Client import SupaWrapper
from .Direct import DirectConnection


class Connection:
    '''
    Class that manages database connections and requests
    '''
    def __init__(self,
        db:str
    ):
        self.db = db
        self.pandabase_root = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
        self.dbs_locs = '{0}/DBs'.format(self.pandabase_root)
        ## logging ##
        self.logger = logger.get_logger('db.manager')
        ## local location of the database
        self.loc = '{0}/{1}'.format(
            self.dbs_locs,
            self.db
        )
        ## define connection clients ##
        self.credentials: Optional[DBCredentials] = None
        self.supabase: Optional[SupaWrapper] = None
        self.conn: Optional[DirectConnection] = None
        ## initialize ##
        self.check_dir()
        self.init_env()
        self.initialize_connections()

    ############################
    ## INITIALIZATION HELPERS ##
    ############################
    def check_dir(self):
        '''
        Checks the DBs locations for the database. Creates the
        directory if it does not exist.
        '''
        db_dir = '{0}/{1}'.format(  
            self.dbs_locs,
            self.db
        )
        if not os.path.exists(db_dir):
            self.logger.info('Creating directory for {0}'.format(self.db))
            os.makedirs(db_dir)

    def load_global_env(self) -> Optional[DBCredentials]:
        '''
        Loads a global .env at the root of the module and
        looks for DB specific credentials with the format
        <db_name>_SUPABASE_URL
        <db_name>_SUPABASE_KEY
        etc
        '''
        ## establish .env location
        env_loc = '{0}/.env'.format(self.pandabase_root)
        if os.path.exists(env_loc):
            self.logger.info('Loading global .env file')
            load_dotenv(env_loc)
        ## load credentials ##
        var_prefix = '{0}_'.format(self.db.upper())
        try:
            self.logger.info('Loading credentials for {0}'.format(self.db))
            return DBCredentials(
                supabase_url=os.getenv('{0}SUPABASE_URL'.format(var_prefix)),
                supabase_key=os.getenv('{0}SUPABASE_KEY'.format(var_prefix)),
                db_host=os.getenv('{0}DB_HOST'.format(var_prefix)),
                db_name=os.getenv('{0}DB_NAME'.format(var_prefix)),
                db_user=os.getenv('{0}DB_USER'.format(var_prefix)),
                db_password=os.getenv('{0}DB_PASSWORD'.format(var_prefix)),
                db_port=int(os.getenv('{0}DB_PORT'.format(var_prefix), 5432))
            )
        except Exception as e:
            self.logger.error(
                'Failed to load credentials for {0}'.format(self.db),
                extra={'error': str(e)}
            )
            raise e

    def init_env(self):
        '''
        Wrapper that loads the .env and stores in the credentials
        '''
        ## try local first ##
        self.credentials = self.load_global_env()
    
    def initialize_connections(self):
        '''
        Initializes the supabase client and direct connection
        '''
        self.logger.info('Initializing connections for {0}'.format(self.db))
        if self.credentials is None:
            raise Exception('DB ERROR: ({0}) No credentials found. Add to .env in package root'.format(self.db))
        self.supabase = SupaWrapper(self.credentials)
        self.conn = DirectConnection(self.credentials)


    #####################
    ## METHOD WRAPPERS ##
    #####################
    def get_table(self, table_name:str)->pd.DataFrame:
        '''
        Downloads an entire table from supabase as a pandas dataframe

        Parameters:
        * table_name: Name of the table to download

        Returns:
        * pd.DataFrame: A pandas dataframe containing the table data
        '''
        return self.supabase.get_table(table_name)

    def get_table_stats(self, table_name:str)->Dict[str, Any]:
        '''
        Gets metadata for a given table in the database

        Parameters:
        * table_name : name of the table to check
        
        Returns:
        * dict of last_modified and record_count
        '''
        return self.conn.get_table_stats(table_name)

    def get_table_fields(self, table_name:str)->List[Field]:
        '''
        Gets fields for a given table in the database

        Parameters:
        * table_name: Name of the table in the database

        Returns:
        * fields: List of Field objects
        '''
        return self.conn.get_table_fields(table_name)
    
    def upsert(self,
        table_name:str,
        data:Dict[str, Any],
        on_conflict:List[str] = []
    ):
        '''
        Upserts data to the table

        Parameters:
        * table_name: Name of the table to upsert to
        * data: A dictionary containing the data to upsert
        * on_conflict: A list of columns to use for the upsert conflict resolution

        Returns:
        * none
        '''
        self.supabase.upsert(table_name, data, on_conflict)

    def delete(self, table_name:str, primary_keys:List[Dict[str, Any]]):
        '''
        Deletes data from the table

        Parameters:
        * table_name: Name of the table to delete from
        * primary_keys: A list of dictionaries, containing primary key values

        Returns:
        * none
        '''
        self.supabase.delete(table_name, primary_keys)