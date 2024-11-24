from dataclasses import dataclass, asdict
from typing import Dict, Any
from ...DBTypes.table_stats import TableStats

@dataclass
class CacheMetadata:
    stats: TableStats
    data_hash: str

    @classmethod
    def from_dict(cls, data:Dict[str, Any])->'CacheMetadata':
        return cls(
            stats=TableStats.from_dict(data['stats']),
            data_hash=data['data_hash']
        )

    def asdict(self) -> Dict[str, Any]:
        ## need to specify the dict instead of using the built in asdict(self) ##
        ## because the TableStats class has a timestamp that needs to be converted to a string ##
        ## as a datetime object cannot be json serialized ##
        return {
            'stats': self.stats.asdict(),
            'data_hash': self.data_hash
        }