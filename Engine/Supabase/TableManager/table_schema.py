## data imports ##
import pandas as pd

## logging ##
from pandabase.Logging import logger

## types ##
from typing import Dict, Any, List
from ..DBTypes import Field

class TableSchema:
    '''
    Represents the schema of a table. Contains a list of fields
    which are instances of the Field class.
    '''
    def __init__(self, table_name:str, fields: List[Field]):
        self.table_name = table_name
        self.fields = fields
        self.logger = logger.get_logger('table.schema')
        self.pks, self.uniques = self.get_conflict_columns()
        self.schema = self.to_dict()
        self.pd_schema = self.to_pd_schema()
        self.pd_dtypes = self.to_pd_dtypes()
        ## ensure an updated_at field is present ##
        if 'updated_at' not in [field.name for field in self.fields]:
            self.logger.error(
                'updated_at is required by pandabase, but was not found in {0} schema'.format(self.table_name)
            )
            raise ValueError(
                'updated_at is required by pandabase, but was not found in {0} schema'.format(self.table_name)
            )
    
    def to_dict(self) -> Dict[str, Any]:
        '''
        Converts the schema to a dictionary. For storing locally.
        '''
        return {
            'table_name': self.table_name,
            'fields': [field.asdict() for field in self.fields]
        }

    def to_pd_schema(self) -> Dict[str, Any]:
        '''
        Converts the schema to a schema for initializing an pandas dataframe.
        '''
        return {
            field.name: pd.Series(dtype=field.pd_dtype) for field in self.fields
        }

    def to_pd_dtypes(self) -> Dict[str, Any]:
        '''
        Converts the schema to a dictionary of pd dtypes for reading a local csv
        which may be empty and cant have dtypes inferred
        '''
        return {
            field.name: field.pd_dtype for field in self.fields
        }
    
    def get_conflict_columns(self) -> List[str]:
        '''
        Returns primary keys for upsert conflicts and all unique columns for
        validation purposes.
        '''
        # First check for primary key
        pks = [
            field.name for field in self.fields 
            if field.is_primary_key
        ]
        ## then all unique columns ##
        uniques = [
            field.name for field in self.fields
            if field.is_primary_key or field.has_unique_constraint
        ]
        ## return ##
        return pks, uniques

    def validate_df_schema(self, df:pd.DataFrame) -> bool:
        '''
        Compares the schema of the table to a dataframe and returns True if they match

        Parameters:
        * df: a pandas dataframe

        Returns:
        * bool: True if the schema matches, False otherwise
        '''
        ## state ##
        conforming = True
        issues = []
        ## compare the fields ##
        self.logger.info(
            'Validating dataframe schema against {0} schema'.format(self.table_name)
        )
        for field in self.fields:
            ## check if the field is in the df ##
            if field.name not in df.columns:
                conforming = False
                issues.append('Column {0} not found in df'.format(field.name))
            else:
                ## if the field is in the df, check that the dtype matches ##
                validation_issues = field.validate_series(df[field.name])
                if len(validation_issues) > 0:
                    conforming = False
                    issues.extend(validation_issues)
        ## return ##
        if conforming:
            self.logger.info('Dataframe schema conforms to {0} schema'.format(self.table_name))
            return True
        else:
            self.logger.error(
                'Dataframe schema does not conform to {0} schema'.format(self.table_name),
                extra={'error': ', '.join(issues)}
            )
            return False
        