import pandas as pd

## logging ##
from pandabase.Logging import logger

## typing imports ##
from typing import Optional, Dict, Any, List
from ...DBTypes import DBCredentials

## supabase imports ##
from supabase import create_client, Client

class SupaWrapper:
    '''
    Wrapper for the supabase client that provides abstracted
    methods for relevant operations like bulk downloads and upserts
    '''
    def __init__(self, credentials:DBCredentials):
        self.credentials:DBCredentials = credentials
        self.supabase:Optional[Client] = None
        self.logger = logger.get_logger('db.client')
        self.connect()

    def connect(self):
        '''
        Connects the supabase client
        '''
        try:
            self.logger.info('Connecting to supabase')
            self.supabase = create_client(
                self.credentials.supabase_url,
                self.credentials.supabase_key
            )
            self.logger.info('Connected to supabase')
        except Exception as e:
            self.logger.error(
                'Failed to connect to supabase',
                extra={
                    'error': str(e)
                })
            raise e

    def check_client(self) -> None:
        '''
        Checks if client was successfully connected to supabase
        '''
        if not self.supabase:
            ## if client is not connected, try to reconnect
            self.logger.info('Client disconnected, attempting to reconnect')
            self.connect()
            if not self.supabase:
                self.logger.error('Supabase client connection failed and returned None')
                raise Exception('CLIENT_ERROR: Failed to create supabase client')
        
    def get_table(self,
        table_name:str,
        page_size:int = 500
    ) -> pd.DataFrame:
        '''
        Downloads an entire table from supabase as a pandas dataframe

        Parameters:
        * table_name: Name of the table to download
        * page_size: Number of records to download per page

        Returns:
        * pd.DataFrame: A pandas dataframe containing the table data
        '''
        ## check connection ##
        self.check_client()
        ## initialize variables ##
        all_records = []
        start = 0
        self.logger.info('Downloading table: {0}'.format(table_name))
        try:
            while True:
                ## paginated queries ##
                resp = self.supabase.table(table_name)\
                    .select('*')\
                    .range(start, start + page_size)\
                    .execute()
                records = resp.data
                ## end if no records are returned ##
                if len(records) == 0:
                    break
                ## add records to the list ##
                all_records.extend(records)
                ## increment the start ##
                start += page_size
                ## check if we reached end ##
                if len(records) < page_size:
                    break
        except Exception as e:
            self.logger.error(
                'CLIENT_ERROR: Failed to download table',
                extra={
                    'error': str(e)
                })
            raise e
        ## return the dataframe ##
        if len(all_records) > 0:
            self.logger.info('Downloaded {0} records from {1}'.format(
                len(all_records),
                table_name
            ))
            return pd.DataFrame(all_records)
        else:
            self.logger.info('No records found in {0}'.format(table_name))
            return pd.DataFrame()

    def upsert(self,
        table_name:str,
        data:List[Dict[str, Any]],
        on_conflict:List[str] = [],
        page_size:int = 500
    ) -> None:
        '''
        A paginated upsert to a target table. Validation is performed
        by the Table class, which is where this method should be used.

        Parameters:
        * table_name: Name of the table to upsert to
        * data: A dictionary containing the data to upsert
        * on_conflict: A list of the table's primary keys
        * page_size: Number of records to upsert per page

        Returns:
        * None
        '''
        ## check connection ##
        self.check_client()
        ## initialize variables ##
        total_records = len(data)
        processed = 0
        page_number = 1
        self.logger.info('Upserting {0} records to {1}'.format(
            total_records,
            table_name
        ))
        ## loop through pages ##
        try:
            while processed < total_records:
                ## define batch ##
                batch = data[processed:processed+page_size]
                ## paginated upsert ##
                resp = self.supabase.table(
                    table_name
                ).upsert(
                    batch,
                    count='exact',
                    on_conflict=on_conflict
                ).execute()
                ## unpack response ##
                upserted = resp.count
                ## handle errors ##
                if hasattr(resp, 'error'):
                    raise Exception('Failed to upsert page {0}: {1}'.format(
                        page_number,
                        resp.error
                    ))
                ## else log success and increment ##
                self.logger.info('Upserted page {0} of {1}'.format(
                    page_number,
                    (total_records + page_size - 1) // page_size
                ))
                processed += len(batch)
                page_number += 1
        except Exception as e:
            self.logger.error(
                'Failed to upsert',
                extra={
                    'error': str(e)
                })
            raise e

    def delete(self,
        table_name:str,
        primary_keys:List[Dict[str, Any]],
        page_size:int = 30
    ) -> None:
        '''
        Deletes an array of records, defined by their primary keys
        To delete from a table with a composite primary key,
        each key should be represented in the dictionary.

        Note, since we must use a filter for composite keys, and
        the filter must be encoded in the URL, we limit batch size
        to 30 records. Delete should be used for very large bulk
        deletions.

        Parameters:
        * table_name: Name of the table to delete from
        * primary_keys: A list of dictionaries, containing primary key values
        * page_size: Number of records to delete per page

        Returns:
        * None
        '''
        ## check connection ##
        self.check_client()
        ## initialize ##
        total_records = len(primary_keys)
        processed = 0
        page_number = 1
        self.logger.info('Deleting {0} records from {1}'.format(
            total_records,
            table_name
        ))
        ## paginated delete ##
        try:
            while processed < total_records:
                ## get batch ##
                batch = primary_keys[processed:processed+page_size]
                ## determine if single or composite key ##
                if len(batch[0].keys()) == 1:
                    ## if single key, use normal delete with an in ##
                    key_name = list(batch[0].keys())[0]
                    ## create key list to delete ##
                    key_values = [pk[key_name] for pk in batch]
                    ## create query ##
                    query = (
                        self.supabase.table(table_name)
                        .delete()
                        .in_(key_name, key_values)
                    )
                else:
                    ## if composite key, create and condition for each key ##
                    conditions = []
                    for rec in batch:
                        ## create and conditions for each key ##
                        and_conditions = []
                        for key_name, key_value in rec.items():
                            and_conditions.append('{0}.eq.{1}'.format(key_name, key_value))
                        ## add and conditions to the list ##
                        conditions.append(
                            ' and '.join(and_conditions)
                        )
                    ## Combine batch conditions with or ##
                    where_clause = '({0})'.format(') or ('.join(conditions))
                    ## create query ##
                    query = (
                        self.supabase.table(table_name)
                        .delete()
                        .or_(where_clause)
                    )
                ## execute delete ##
                resp = query.execute()
                ## check response ##
                if hasattr(resp, 'error'):
                    raise Exception('Failed to delete page {0}: {1}'.format(
                        page_number,
                        resp.error
                    ))
                ## log success and increment ##
                self.logger.info('Deleted page {0} of {1}'.format(
                    page_number,
                    (total_records + page_size - 1) // page_size
                ))
                processed += len(batch)
                page_number += 1
        except Exception as e:
            self.logger.error(
                'Failed to delete',
                extra={'error': str(e)}
            )
            raise e
        
