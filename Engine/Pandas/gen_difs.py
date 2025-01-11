import pandas as pd
import numpy
from typing import Tuple, Optional, List, Dict, Any

def rows_are_equal(
        source_row:pd.Series,
        destination_row:pd.Series,
        dtypes:Dict[str, numpy.dtype],
        ignore_columns: List[str] = ['created_at', 'updated_at']
    ) -> bool:
    '''
    Checks if two rows are equal while leaving wiggle room for non-meaninful
    differences in precision, null representation, and column ordering.

    Note that at this time, this function does not check for data within
    objects, like json, point, etc. It will compare those as strings.

    Parameters:
    * source_row: The source row to compare
    * destination_row: The destination row to compare against

    Returns:
    * are_equal: True if the rows are equal, False otherwise
    '''
    ## loop through columns ##
    for col in source_row.index:
        ## skip ignored columns ##
        ## by default, these are the system columns created_at and updated_at ##
        if col in ignore_columns:
            continue
        ## get values ##
        s_val = source_row[col]
        d_val = destination_row[col]
        ## check null equality ##
        if pd.isna(s_val) and pd.isna(d_val):
            continue
        ## check for null mismatch ##
        if pd.isna(s_val) != pd.isna(d_val):
            return False
        ## check for numeric mismatch ##
        if pd.api.types.is_numeric_dtype(
            dtypes[col]
            if isinstance(dtypes[col], numpy.dtype)
            else 'fail'
        ):
            if not numpy.isclose(s_val, d_val, rtol=1e-05, atol=1e-08):
                return False
        ## check time mismatch ##
        if pd.api.types.is_datetime64_any_dtype(
            dtypes[col]
            if isinstance(dtypes[col], numpy.dtype)
            else 'fail'
        ):
            if pd.Timestamp(s_val) != pd.Timestamp(d_val):
                return False
        ## standard comparison ##
        if s_val != d_val:
            # Time objects (dates, timestamps, etc) may be held as strings in source dataframe and still
            # pass validation against DB schema because they are parsable as timestamps.
            # Row comparison relies on the source dataframe's dtypes, meaning it is possible that a
            # a timestamp column is treated as a string column. This may not reliably pass standard comparison
            # because pandas and postgres to not necessarily serialize and deserialize timestamps to the
            # same format, and string conversion of timestamps can introduce formatting differences as well.
            # For instance, "2024-01-01T00:00:00+00:00" will not be deemed equal to "2024-01-01 00:00:00+00:00"
            # If the source dataframe dtypes have interpreted the column as a string (ie object)
            # Ideally, formatting is handled upstream by the user, but as a fallback, an additional
            # check is done here to ensure we do not create an unnecessary upsert based only on malformed
            # timestamps
            try:
                if isinstance(s_val, str) and isinstance(d_val, str):
                    ## if its an object that can be converted to a timestamp, return
                    ## the comparison of the converted values
                    return pd.to_datetime(s_val) == pd.to_datetime(d_val)
            except (ValueError, TypeError):
                ## if it cannot be converted to a timestamp, proceed with standard comparison
                pass
            return False
    ## if all checks pass, the rows are equal ##
    return True

def gen_diffs(
        df_source:pd.DataFrame,
        df_destination:Optional[pd.DataFrame],
        primary_keys:List[str]
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    '''
    Generates an upsert and delete dataframe based on the difference between
    the source and destination dataframes

    Parameters:
    * df_source: The source dataframe to be mirrored by the destination
    * df_destination: The destination dataframe to compare the source against
    * primary_keys: The primary keys to use for comparison

    Returns:
    * upsert_df: A dataframe of records to be upserted to the destination
    * delete_df: A dataframe of records to be deleted from the destination
    '''
    ## handle no destination case ##
    ## this occurs when we have no cache or the dest has no records ##
    if df_destination is None:
        return df_source, None
    ## create dictionaries of record indices keyed by primary keys ##
    source_index_by_pk = {
        tuple(row[primary_keys]) : idx
        for idx, row in df_source.iterrows()    
    }
    destination_index_by_pk = {
        tuple(row[primary_keys]) : idx
        for idx, row in df_destination.iterrows()
    }
    ## get dtypes for downstream dtype lookup ##
    dtypes = {col: df_source[col].dtype for col in df_source.columns}
    ## find new and changed records (ie upserts) ##
    upsert_idx = set()
    for pk, idx in source_index_by_pk.items():
        ## new record ##
        if pk not in destination_index_by_pk:
            upsert_idx.add(idx)
        else:
            ## existing record ##
            destination_idx = destination_index_by_pk[pk]
            ## if rows are different, consider it an upsert ##
            if not rows_are_equal(
                df_source.iloc[idx],
                df_destination.iloc[destination_idx],
                dtypes=dtypes
            ):
                upsert_idx.add(idx)
    ## find deleted records ##
    delete_idx = set(
        dest_idx for pk, dest_idx in destination_index_by_pk.items()
        if pk not in source_index_by_pk
    )
    ## filter dfs ##
    upsert_df = df_source.iloc[list(upsert_idx)].copy() if upsert_idx else None
    delete_df = df_destination.iloc[list(delete_idx)].copy() if delete_idx else None
    ## return upserts and deletes ##
    return upsert_df, delete_df