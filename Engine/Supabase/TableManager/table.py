## system ##
import os
import json
import datetime

## data imports ##
import pandas as pd

## logging ##
from pandabase.Logging import logger

## types and classes ##
from typing import Dict, Any, Optional, List
from .table_schema import TableSchema
from ..DBTypes import Field, TableStats
from ..ConnectionManager import Connection
from .CacheManager import CacheManager

## decorators ##
from dataclasses import dataclass


class Table:
    '''
    Class that manages tables associated with a DB. The data contained
    in the table, and its associated metadata, represent the data as it
    exists in the DB, or, if built from the local cache, as we expect it
    to exist in the DB

    Parameters:
    * db: a Connection object
    * table: the name of the table
    '''
    def __init__(self, db:Connection, table:str):
        ## init attributes ##
        self.db = db
        self.table = table
        self.loc = '{0}/{1}'.format(
            self.db.loc,
            self.table
        )
        self.cache: CacheManager = CacheManager(self.table, self.loc)
        self.schema: Optional[TableSchema] = None
        self.data: Optional[pd.DataFrame] = None
        self.meta: Optional[TableStats] = None
        self.cache_valid: Optional[bool] = None
        self.logger = logger.get_logger('table.manager')
        ## init functions ##
        self.check_dir()
        self.get_schema()
        
    def check_dir(self):
        '''
        Checks the DB for the table's local directory. Creates the
        directory if it does not exist.
        '''
        if not os.path.exists(self.loc):
            self.logger.info('Creating directory for {0} table'.format(self.table))
            os.makedirs(self.loc)
    
    ############
    ## SCHEMA ##
    ############
    def get_schema(self):
        '''
        Retrieves the schema from the DB and creates a TableSchema object
        '''
        ## get the schema ##
        self.logger.info('Getting schema for {0} table'.format(self.table))
        table_fields = self.db.get_table_fields(self.table)
        ## create the schema ##
        self.logger.info('Creating TableSchema object for {0} table'.format(self.table))
        self.schema = TableSchema(self.table, table_fields)
        ## save a local json copy of the schema ##
        self.logger.info('Saving local copy of {0} table schema'.format(self.table))
        with open('{0}/schema.json'.format(self.loc), 'w') as f:
            json.dump(
                self.schema.schema,
                f,
                indent=4
            )
    
    def validate_df(self, df:pd.DataFrame):
        '''
        Validates a dataframe against the table schema

        Parameters:
        * df: a pandas dataframe

        Returns:
        * bool: True if the dataframe conforms to the schema, False otherwise
        '''
        return self.schema.validate_df_schema(df)
    
    def set_df(self, df:pd.DataFrame):
        '''
        Sets the tables data with a DF, and performs validation to ensure
        type compliance. Data should only be updated with set_df

        Parameters:
        * df: a pandas dataframe
        '''
        ## validate ##
        if not self.validate_df(df):
            self.logger.error(
                'Table data tried to be set with a Dataframe that does not conform to schema',
                extra={'error': 'non conforming dataframe for table {0}'.format(self.table)}
            )
            raise ValueError('Table data tried to be set with a Dataframe that does not conform to schema')
        ## set ##
        self.data = df
    
    ############
    ## CACHE ##
    ############
    def get_table_stats(self) -> TableStats:
        '''
        Gets the table stats from the DB

        Returns:
        * TableStats: an object containing the record count and number of modifications
        '''
        self.logger.info('Getting table stats for {0} table'.format(self.table))
        stats = self.db.get_table_stats(self.table)
        return TableStats.from_dict(stats)

    def pull_table_data(self):
        '''
        Pulls the table data from the DB. This is called when the cache is invalidated and
        the table needs to be refreshed.
        '''
        ## pull from the db ##
        self.logger.info('Rebuilding {0} table'.format(self.table))
        self.set_df(self.db.get_table(self.table))
        self.meta = self.get_table_stats()
    
    def validate_cache(self):
        '''
        Validates the cache, and updates it if it is invalid
        '''
        ## update meta ##
        self.meta = self.get_table_stats()
        ## validate the cache ##
        cache_valid = self.cache.validate_cache(self.meta)
        if not cache_valid:
            self.logger.info('Cache is invalid, rebuilding')
            ## get fresh data ##
            self.pull_table_data()
            ## update the cache ##
            self.cache.update_cache(
                self.meta, self.data
            )
            ## validate ##
            self.cache_valid = True
        else:
            self.logger.info('Cache is valid, using local copy')
            ## load the cache ##
            self.set_df(self.cache.local_df)
            self.meta = self.cache.local_stats
            ## validate ##
            self.cache_valid = True
    
    def update_cache(self, df:pd.DataFrame):
        '''
        Updates the cache with a new df

        Parameters:
        * df: a pandas dataframe
        '''
        self.logger.info('Updating cache for {0} table'.format(self.table))
        self.set_df(df)
        self.meta = self.get_table_stats()
        self.cache.update_cache(self.meta, df)
        self.cache_valid = True

    ###########
    ## CRUDs ##
    ###########
    def upsert(self, data:Dict[str, Any]):
        '''
        Upserts data to the table
        '''
        self.db.upsert(
            self.table,
            data,
            on_conflict=self.schema.pks
        )
    
    def delete(self, primary_keys:List[Dict[str, Any]]):
        '''
        Deletes data from the table
        '''
        ## check that the primary keys are valid ##
        ## determine which primary keys were passed ##
        self.logger.info('Validating primary keys for {0} table delete operation'.format(self.table))
        try:
            pks_passed = []
            for pk in primary_keys:
                for k, v in pk.items():
                    if k not in pks_passed:
                        pks_passed.append(k)
            ## validate the primary keys ##
            for pk_passed in pks_passed:
                ## ensure the passed pk is in the schema ##
                if pk_passed not in self.schema.pks:
                    raise ValueError('Primary key {0} not in schema'.format(pk_passed))
                for pk in self.schema.pks:
                    ## first ensure needed pks were passed ##
                    if pk not in pks_passed:
                        raise ValueError('Missing primary key: {0}'.format(pk))
        except ValueError as e:
            self.logger.error(
                'PKs for delete were invalid',
                extra={'error': e}
            )
            raise e
        ## delete the data, and return the response ##
        self.db.delete(
            self.table,
            primary_keys
        )