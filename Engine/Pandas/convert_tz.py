import pandas as pd

def convert_tz(
        df:pd.DataFrame,
        tz:str = 'America/Los_Angeles'
    ) -> pd.DataFrame:
    '''
    Converts all time columns to the specified timezone, and then
    to string, so it can be serialized to JSON.

    Parameters:
    * df: DataFrame to convert
    * tz: Timezone to convert to

    Returns:
    * df: DataFrame with time columns converted to the specified timezone and then to string
    '''
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            ## convert to TZ aware UTC
            df[col] = pd.to_datetime(df[col])  # Convert to datetime
            df[col] = df[col].dt.tz_convert('UTC')  # Convert to UTC first
            df[col] = df[col].dt.tz_convert(tz)  # Convert to tz
            ## convert to string ##
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    return df
