from dataclasses import dataclass
from typing import Dict, Any
from datetime import datetime

@dataclass
class TableStats:
    '''
    Stats about a table used for local cache validation. If the data on the
    DB does not match this expectation, the cache is invalidated.

    TableStats are pulled when a table class is instantiated and compared to 
    a locally written copy of the stats. After a successful database operation from the package
    the table stats are repulled and the local copy is updated alongside the dataframe that generated
    the database operations (as the cache).

    Note, supabase does not support atomic transactions, and local copies of the data
    and stats are not updated unless all operations are successful. Therefore, if the
    package suffers a partial failure, the cache will definitionally be invalidated
    the next time the table is instantiated, which will trigger a complete refresh
    of the local data and stats
    '''
    last_modified: datetime
    record_count: int
    total_modifications: int

    def __post_init__(self):
        '''
        Post initialization logic -- special handling for empty data that
        occurs when a table is empty in supabase
        '''
        if self.last_modified is None:
            self.last_modified = datetime.fromisoformat('1970-01-01T00:00:00+00:00')
        
        if self.record_count is None:
            self.record_count = 0

        if self.total_modifications is None:
            self.total_modifications = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any])->'TableStats':
        '''
        Creates a TableStats object from a dictionary
        '''
        ## check for empty data ##
        if not data:
            raise ValueError('No data provided to create TableStats object')
        ## create the object ##
        try:
            ## convert last_modified to datetime if it was provided as a string ##
            if isinstance(data['last_modified'], str):
                data['last_modified'] = datetime.fromisoformat(data['last_modified'])
            ## return the object ##
            return cls(
                last_modified=data['last_modified'],
                record_count=data['record_count'],
                total_modifications=data['total_modifications']
            )
        except KeyError as e:
            raise ValueError('Missing key required to create TableStats object: {0}'.format(e))
        except ValueError as e:
            raise ValueError('Error creating TableStats object: {0}'.format(e))
        
    def validate(self, other: 'TableStats')->bool:
        '''
        Validates that the current TableStats object matches the provided TableStats object
        '''
        ## track issues for logging ##
        issues = []
        ## validate ##
        if self.last_modified != other.last_modified:
            issues.append('cached last_modified of {0} does not match new of {1}'.format(
                self.last_modified, other.last_modified
            ))
        if self.record_count != other.record_count:
            issues.append('cached record_count of {0} does not match new of {1}'.format(
                self.record_count, other.record_count
            ))
        if self.total_modifications != other.total_modifications:
            issues.append('cached total_modifications of {0} does not match new of {1}'.format(
                self.total_modifications, other.total_modifications
            ))
        ## if there are no issues, return True ##
        if len(issues) == 0:
            return True, []
        ## otherwise, return False and log the issues ##
        else:
            return False, issues
    
    def asdict(self) -> Dict[str, Any]:
        ## Specify the dict instead of using the built in asdict(self) ##
        ## because the datetime object needs to be converted to a string ##
        ## as a datetime object cannot be json serialized ##
        return {
            'last_modified': self.last_modified.isoformat(),
            'record_count': self.record_count,
            'total_modifications': self.total_modifications
        }