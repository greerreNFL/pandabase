from dataclasses import dataclass, asdict, field
from typing import Dict, Any, List
import numpy
import pandas as pd
import json
import re

## pg to pandas type map ##
from .validity_map import validity_map

## pandas casting maps ##
from .int_downcast_map import int_downcast_map
from .float_downcast_map import float_downcast_map

@dataclass
class Field:
    '''
    Represents a PG Catalog Field Type
    '''
    name:str
    udt_name:str
    data_type:str
    is_nullable:bool=False
    is_primary_key:bool=False
    has_unique_constraint: bool = False
    has_foreign_key:bool=False
    has_default:bool=False
    default_value:str=None
    pd_dtype:str=field(init=False)

    def __post_init__(self):
        '''
        Post initialization logic. Create a pd_dtype field for reference of 
        valid dtype. This is used if the package needs to create an empty/dummy
        df based on the schema for tables that have no existing data in the DB
        '''
        dtype_array = validity_map.get(self.udt_name.lower(), ['object'])
        if len(dtype_array) == 0:
            self.pd_dtype = 'object'
        else:
            self.pd_dtype = dtype_array[0]
    
    @classmethod
    def from_record(cls, record: Dict[str, Any]) -> 'Field':
        '''
        Create Field from database record
        '''
        return cls(
            name=record['column_name'],
            udt_name=record['udt_name'],
            data_type=record['data_type'],
            is_nullable=record['is_nullable'] == 'YES',
            is_primary_key=record['constraint_type'] == 'PRIMARY KEY',
            has_unique_constraint=record['constraint_type'] == 'UNIQUE',
            has_default=record['column_default'] is not None,
            default_value=record['column_default']
        )

    def asdict(self) -> Dict[str, Any]:
        '''
        Convert the field to a dictionary
        '''
        return asdict(self)

    #########################
    ## VALIDATION METHODS ##
    #########################
    def valid_int_downcasts(self, series:pd.Series) -> List[str]:
        '''
        For an int dtype, get a list of valid downcasts to determine
        the set of int types the pandas type, which defaults to a wider int range, can
        be downcast to by pg
        '''
        ## container for valid downcasts ##
        valid_downcasts = []
        ## handle empty ##
        min_val = 0 if len(series.dropna()) == 0 else series.min()
        max_val = 0 if len(series.dropna()) == 0 else series.max()   
        for dtype, range_dict in int_downcast_map.items():
            if range_dict['min'] <= min_val and range_dict['max'] >= max_val:
                valid_downcasts.append(dtype)
        ## handle potentiality for int casting by pg ##
        ## ints can always be cast to float so long as the range is not too large ##
        for dtype, range_dict in float_downcast_map.items():
            if range_dict['min'] <= min_val and range_dict['max'] >= max_val:
                valid_downcasts.append(dtype)
        return valid_downcasts

    def valid_float_downcasts(self, series:pd.Series) -> List[str]:
        '''
        For an float dtype, get a list of valid downcasts to determine
        the set of float types the pandas type, which defaults to a wider float range, can
        be downcast to by pg

        This also handles instances where the series is a float64 due to nulls, in which case
        the data is parsed to determine if the non-null values 1) have no residuals and data
        will not be lost when assigned to int and 2) are within the range of the int type

        If the series is entirely null, it can be cast to any numeric type that allows nulls
        without losing data and therefore is accepted as a valid cast
        '''
        ## container for valid downcasts ##
        valid_downcasts = []
        ## handle empty or all nan -- if the float is empty, the data can be any ##
        ## type of float ##
        min_val = 0.0 if len(series.dropna()) == 0 else series.min()
        max_val = 0.0 if len(series.dropna()) == 0 else series.max()
        for dtype, range_dict in float_downcast_map.items():
            if range_dict['min'] <= min_val and range_dict['max'] >= max_val:
                valid_downcasts.append(dtype)
        ## handle potentiality for int casting by pg ##
        if (series - numpy.floor(series)).sum() == 0:
            ## if the float type does not contain any information above it's int counterpart,
            ## allow the float to be cast as int ##
            for dtype, range_dict in int_downcast_map.items():
                if range_dict['min'] <= min_val and range_dict['max'] >= max_val:
                    valid_downcasts.append(dtype)
        return valid_downcasts

    def valid_timestamp_cast(self, series:pd.Series) -> bool:
        '''
        Validates that a series can be cast as a timestamp type
        '''
        try:
            pd.to_datetime(series)
            return True
        except:
            return False
        
    def type_validation(self, series:pd.Series) -> bool:
        '''
        Checks if the dtype of a pandas series conforms to the pg field
        '''
        ## get a lowercase udt name ##
        udt_lower = self.udt_name.lower()
        ## get valid types from the validity map ##
        ## note, custom types (like an enum)will not be in the map and
        ## therefore will not be validated ##
        valid_dtypes = validity_map.get(udt_lower, [])
        if len(valid_dtypes) == 0:
            ## again, custom types will be skipped as valid ##
            return True
        ## special logic for all null fields ##
        ## if the pg field is a numeric that allows nulls, an all null series can
        ## be cast without losing data and should be accepted as valid ##
        ## This is a material consideration since an all null series may be read
        ## from the cache as an object, and would not trigger an int or float cast
        ## check.
        numeric_dtypes = ['int16', 'int32', 'int64', 'float32', 'float64']
        if any(dtype in numeric_dtypes for dtype in valid_dtypes):
            ## if field accepts numeric types, perform an all null check ##
            if series.isnull().all():
                return True
        ## same is true in reverse -- if an all null series is read as a numeric when it
        ## its destination type is a varchar (that accepts nulls), it would fail a dtype test
        ## even though it can be cast into the field.
        if any(dtype in ['object', 'string'] for dtype in valid_dtypes):
            if series.isnull().all():
                return True
        ## special logic for pandas numeric casting ##
        castable_dtypes = []
        ## INT ##
        if pd.api.types.is_integer_dtype(series):
            castable_dtypes = self.valid_int_downcasts(series)
            for castable_dtype in castable_dtypes:
                if castable_dtype in valid_dtypes:
                    return True
            return False
        ## FLOAT ##
        if pd.api.types.is_float_dtype(series):
            castable_dtypes = self.valid_float_downcasts(series)
            for castable_dtype in castable_dtypes:
                if castable_dtype in valid_dtypes:
                    return True
            return False
        ## TIMESTAMP and DATE ##
        if self.udt_name.lower() in ['timestamp', 'timestamptz', 'date']:
            return self.valid_timestamp_cast(series)
        ## for dtypes that are not castable, make a direct check ##
        return str(series.dtype) in valid_dtypes

    def json_validation(self, series: pd.Series) -> bool:
        '''
        Validates that all non-null values in the series are valid JSON
        (either already JSON objects/lists or valid JSON strings)
        '''
        ## only for json type ##
        if self.udt_name.lower() not in ['json', 'jsonb']:
            return True
        try:
            # Check each non-null value
            for value in series.dropna():
                if isinstance(value, (dict, list)):
                    # Already a Python object, should serialize fine
                    continue
                elif isinstance(value, str):
                    # Try to parse JSON string
                    json.loads(value)
                else:
                    return False
            return True
        except json.JSONDecodeError as e:
            return False

    def point_validation(self, series: pd.Series) -> bool:
        '''
        Validates that all non-null values in the series are valid 
        PostgreSQL points (either tuples of two floats or valid point strings)
        Format: (x,y)
        '''
        ## only for point type ##
        if self.udt_name.lower() != 'point':
            return True
        ## compile the point pattern ##
        point_pattern = re.compile(r'^\s*\(\s*-?\d+\.?\d*\s*,\s*-?\d+\.?\d*\s*\)\s*$')
        ## check each value ##
        for value in series.dropna():
            if isinstance(value, tuple) and len(value) == 2:
                try:
                    float(value[0]), float(value[1])
                    continue
                except (TypeError, ValueError):
                    return False
            elif isinstance(value, str):
                if not point_pattern.match(value):
                    return False
            else:
                return False
        return True

    def validate_series(self, series:pd.Series) -> bool:
        '''
        Checks if the data in a pandas series conforms to the pg
        field and will write successfully
        '''
        ## container for tracking issues
        issues = []
        ## check nullability
        if not self.is_nullable and series.hasnans:
            issues.append('Column {0} is not nullable, but df has nans'.format(
                self.name
            ))
        ## check unique constraint ##
        if self.has_unique_constraint and series.duplicated().any():
            issues.append('Column {0} has a unique constraint, but df has duplicates'.format(
                self.name
            ))
        ## check dtype ##
        if not self.type_validation(series):
            issues.append('Column {0} dtype {1} does not match field type {2}'.format(
                self.name, str(series.dtype), self.udt_name
            ))
        ## special checks for json, point ##
        if not self.json_validation(series):
            issues.append('Column {0} contains invalid JSON values'.format(self.name))
        if not self.point_validation(series):
            issues.append('Column {0} contains invalid point values'.format(self.name))
        ## return ##
        return issues





