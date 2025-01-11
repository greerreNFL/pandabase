## builtins
import os
import hashlib
import json
import copy
## data ##
import pandas as pd

## logging ##
from pandabase.Logging import logger

## types and classes ##
from typing import Optional
from ...DBTypes.table_stats import TableStats
from .cache_metadata import CacheMetadata

class CacheManager:
    '''
    Class that manages cache operations for a table, including
    reads, writes, and validation
    '''
    def __init__(self, table:str, table_loc:str) -> None:
        ## table name and location
        self.table = table
        self.table_loc = table_loc
        ## cache data ##
        self.local_df: Optional[pd.DataFrame] = None
        self.local_stats: Optional[TableStats] = None
        self.local_hash: Optional[str] = None
        ## logger ##
        self.logger = logger.get_logger('table.cache_manager')
        ## init ##
        self.load_cache()
    
    def load_cache(self) -> None:
        '''
        Loads the cache for the table if it exists
        '''
        ## load the stored dataframe ##
        try:
            self.local_df = pd.read_csv(
                '{0}/data.csv'.format(self.table_loc)
            )
        except FileNotFoundError:
            self.logger.warning('No cache found for table {0}'.format(self.table))
        except Exception as e:
            self.logger.warning(
                'Found cache, but failed to load for table {0}'.format(self.table),
                extra={'error': e}
            )
        ## load the stored metadata ##
        try:
            with open('{0}/cache_metadata.json'.format(self.table_loc), 'r') as f:
                cached_metadata = CacheMetadata.from_dict(
                    json.load(f)
            )
                self.local_stats = cached_metadata.stats
                self.local_hash = cached_metadata.data_hash
        except FileNotFoundError:
            self.logger.warning('No cache metadata found for table {0}'.format(self.table))
            ## invalidate the partial cache if loaded ##
            self.local_df = None
        except Exception as e:
            self.logger.warning(
                'Found cache metadata, but failed to load for table {0}'.format(self.table),
                extra={'error': e}
            )
            ## invalidate the partial cache if loaded ##
            self.local_df = None

    def calculate_hash(self, df:pd.DataFrame) -> str:
        '''
        Creates a SHA-256 hash for the provided dataframe
        '''
        return hashlib.sha256(
            df.to_csv(index=False).encode()
        ).hexdigest()

    def validate_cache(self, external_stats:TableStats)->bool:
        '''
        Validates the cache for the table by comparing number of records
        and total modifications from TableStats, and by checking the hash
        of the local dataframe against the stored hash

        Parameters:
        * external_stats: a TableStats object representing the table in the DB

        Returns:
        * bool: True if the cache is valid, False otherwise
        '''
        ## log ##
        self.logger.info('Validating cache for table {0}'.format(self.table))
        ## if any cache information is missing, it is not valid
        if self.local_df is None or self.local_stats is None or self.local_hash is None:
            self.logger.warning('Cache for table {0} was incomplete and therefore invalidated'.format(self.table))
            return False
        ## validate the table stats ##
        stats_valid, issues = self.local_stats.validate(external_stats)
        if not stats_valid:
            self.logger.warning(
                'Cache for table {0} was invalidated due to mismatching table stats: {1}'.format(
                    self.table,
                    ', '.join(issues)
                )
            )
            return False
        ## validate same lengths in the db and cached dataframe ##
        if len(self.local_df) != external_stats.record_count:
            self.logger.warning(
                'Cache for table {0} was invalidated due to mismatching record counts: (local: {1}, db: {2})'.format(
                    self.table,
                    len(self.local_df),
                    external_stats.record_count
                )
            )
            return False
        ## validate the hash by comparing the loaded df to the stored hash ##
        if self.local_hash != self.calculate_hash(self.local_df):
            self.logger.warning('Cache for table {0} was invalidated due to mismatching hash'.format(self.table))
            return False
        ## if all checks pass, the cache is valid ##
        return True

    def update_cache(self, stats:TableStats, df:pd.DataFrame)->None:
        '''
        Updates the cache for the table with the provided dataframe and table stats
        '''
        ## log ##
        self.logger.info('Updating cache for table {0}'.format(self.table))
        ## current metadata for rollback on failure ##
        current_df = self.local_df.copy() if self.local_df is not None else None
        current_stats = copy.deepcopy(self.local_stats) if self.local_stats is not None else None
        current_hash = self.local_hash  # Strings are immutable, so no need to copy
        ## state for determining success ##
        success = False
        try:
            ## update the local dataframe ##
            self.local_df = df
            ## update the local stats ##
            self.local_stats = stats
            ## update the local hash ##
            self.local_hash = self.calculate_hash(df)
            ## check conformity with the CachedMetadata type ##
            new_meta = CacheMetadata(stats, self.local_hash)
            ## if new meta was created successfully, proceed ##
            success = True
        except Exception as e:
            ## log ##
            self.logger.error(
                'Failed to update cache for table {0}'.format(self.table),
                extra={'error': e}
            )
            ## rollback ##
            self.local_df = current_df
            self.local_stats = current_stats
            self.local_hash = current_hash
        ## if the update was successful, save the new metadata ##
        if success:
            ## save the updated dataframe ##
            self.local_df.to_csv('{0}/data.csv'.format(self.table_loc), index=False)
            ## save the updated metadata ##
            with open('{0}/cache_metadata.json'.format(self.table_loc), 'w') as f:
                json.dump(
                    CacheMetadata(stats, self.local_hash).asdict(),
                    f,
                    indent=4
                )