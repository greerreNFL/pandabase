## data imports ##
import pandas as pd

## logging ##
from pandabase.Logging import logger

## local imports ##
from .Pandas import gen_diffs, convert_tz
from .Supabase import Connection, Table


class SupaDataFrame:
    '''
    Adds a supabase table to a Pandas DataFrame to enable abstracted and
    efficient mirroring to and from the DB.

    init Parameters:
    * df - DataFrame to connect to Supabase
    * db_name - The local name of the database that creds are tied to
    * table_name - The name of the table in the DB to meld with the df
    '''
    def __init__(self,
        df:pd.DataFrame,
        db_name:str,
        table_name:str
    ) -> None:
        ## logging ##
        self.logger = logger.get_logger('supadataframe')
        ## core props ##
        self.df = df.copy()
        self.table_name = table_name
        self.connection = Connection(db_name)
        self.table = Table(self.connection, self.table_name)
        ## additional setup ##
        ## first check df validity to avoid the potential of the cache ##
        ## being invalid and attempting to load the entire table for ##
        ## a mirror that would fail anyway ##   
        self.df_valid = self.table.validate_df(self.df)
        if not self.df_valid:
            raise ValueError('Dataframe does not conform to schema')
        ## validate and load the cache after checking df validity ##
        self.table.validate_cache()

    def mirror(self):
        '''
        Mirror the dataframe to the DB
        '''
        ## log ##
        self.logger.info(
            'Mirroring dataframe to DB for table {0}'.format(self.table_name)
        )
        ## generate difs ##
        upserts, deletes = gen_diffs(
            self.df,
            self.table.data,
            self.table.schema.pks
        )
        ## state for whether upserts and deletes are successful
        ## and cache needs to be updated ##
        re_cache = False
        ## perform upserts ##
        if upserts is not None:
            ## convert any timstamp columns to a serializable isoformat ##
            upserts = convert_tz(upserts)
            self.table.upsert(upserts.to_dict(orient='records'))
            ## if no error thrown, operation was successful ##
            re_cache = True
        else:
            self.logger.info('No upserts to perform')
        ## perform deletes ##
        if deletes is not None:
            self.table.delete(
                deletes[self.table.schema.pks].to_dict(orient='records')
            )
            ## if no error thrown, operation was successful ##
            re_cache = True
        else:
            self.logger.info('No deletes to perform')
        ## update cache if successful changes were made ##
        if re_cache:
            self.table.update_cache(self.df)
        else:
            self.logger.info('No changes to cache')

    
